# [REPLACE] /home/dpwanjala/repositories/cx-shell/src/cx_shell/engine/connector/engine.py

import json
import uuid
import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Optional

import structlog
import yaml
import networkx as nx

from ...utils import CX_HOME
from jinja2 import Environment, TemplateError
from cx_core_schemas.connector_script import ConnectorScript, ConnectorStep
from cx_core_schemas.vfs import RunManifest, StepResult, Artifact
from ...engine.transformer.service import TransformerService
from ...utils import resolve_path
from ...management.cache_manager import CacheManager

from .config import ConnectionResolver

if TYPE_CHECKING:
    from .service import ConnectorService

logger = structlog.get_logger(__name__)
RUNS_DIR = CX_HOME / "runs"


def sql_quote_filter(value):
    if value is None:
        return "NULL"
    return f"'{str(value).replace("'", "''")}'"


class ScriptEngine:
    """Orchestrates the execution of a declarative workflow script with caching and lineage."""

    def __init__(self, resolver: ConnectionResolver, connector: "ConnectorService"):
        self.resolver = resolver
        self.connector = connector
        self.cache_manager = CacheManager()
        RUNS_DIR.mkdir(exist_ok=True, parents=True)

        self.jinja_env = Environment()
        self.jinja_env.filters["sqlquote"] = sql_quote_filter

        def get_now(tz: str | None = None) -> datetime:
            if tz and tz.lower() == "utc":
                return datetime.now(timezone.utc)
            return datetime.now()

        self.jinja_env.globals["now"] = get_now

    def _build_dependency_graph(self, steps: list[ConnectorStep]) -> nx.DiGraph:
        # [This method remains unchanged]
        dag = nx.DiGraph()
        step_map = {step.id: step for step in steps}
        for step in steps:
            dag.add_node(step.id, step_data=step)
            if step.depends_on:
                for dep_id in step.depends_on:
                    if dep_id in step_map:
                        dag.add_edge(dep_id, step.id)
                    else:
                        raise ValueError(
                            f"Step '{step.id}' has an invalid dependency: '{dep_id}'"
                        )
        if not nx.is_directed_acyclic_graph(dag):
            cycle = nx.find_cycle(dag, orientation="original")
            raise ValueError(f"Workflow contains a circular dependency: {cycle}")
        return dag

    def _calculate_cache_key(
        self, step: ConnectorStep, parent_hashes: Dict[str, str]
    ) -> str:
        # [This method remains unchanged]
        hasher = hashlib.sha256()
        step_def_dict = step.model_dump()
        step_def_str = json.dumps(step_def_dict, sort_keys=True)
        hasher.update(step_def_str.encode("utf-8"))
        sorted_parent_hashes = sorted(parent_hashes.items())
        for step_id, hash_val in sorted_parent_hashes:
            hasher.update(f"{step_id}:{hash_val}".encode("utf-8"))
        return f"sha256:{hasher.hexdigest()}"

    def _find_cached_step(self, cache_key: str) -> Optional[StepResult]:
        # [This method remains unchanged]
        try:
            for manifest_file in sorted(
                RUNS_DIR.glob("**/manifest.json"), reverse=True
            )[:100]:
                manifest_data = json.loads(manifest_file.read_text())
                for step_result in manifest_data.get("steps", []):
                    if (
                        step_result.get("cache_key") == cache_key
                        and step_result.get("status") == "completed"
                    ):
                        logger.debug(
                            "engine.cache.hit",
                            cache_key=cache_key,
                            found_in_run=manifest_data.get("run_id"),
                        )
                        return StepResult(**step_result)
        except Exception as e:
            logger.warn("engine.cache.scan_error", error=str(e))
        logger.debug("engine.cache.miss", cache_key=cache_key)
        return None

    async def _execute_step(
        self,
        step: ConnectorStep,
        run_context: Dict,
        session_variables: Dict[str, Any] = None,
        active_session: Any = None,
        stateful_strategy: Any = None,
    ) -> Any:
        # [This method remains unchanged]
        log = logger.bind(step_id=step.id, step_name=step.name)
        base_render_context = {
            "CX_HOME": str(CX_HOME.resolve()),
            "steps": run_context.get("steps", {}),
            "script_input": run_context.get("script_input", {}),
            **(session_variables or {}),
        }
        run_block_dict = step.run.model_dump()
        full_render_context = {**base_render_context, **run_block_dict}

        def recursive_render(data: Any, context: Dict):
            if isinstance(data, dict):
                return {k: recursive_render(v, context) for k, v in data.items()}
            if isinstance(data, list):
                return [recursive_render(i, context) for i in data]
            if isinstance(data, str):
                stripped_data = data.strip()
                if (
                    stripped_data.startswith("{{")
                    and stripped_data.endswith("}}")
                    and stripped_data.count("{{") == 1
                ):
                    expression = stripped_data[2:-2].strip()
                    try:
                        return self.jinja_env.compile_expression(expression)(**context)
                    except Exception:
                        pass
                if "{{" in data:
                    try:
                        return self.jinja_env.from_string(data).render(**context)
                    except TemplateError as e:
                        raise ValueError(f"Jinja rendering failed: {e}")
            return data

        try:
            rendered_step_data = recursive_render(
                step.model_dump(), full_render_context
            )
            validated_step = ConnectorStep(**rendered_step_data)
            action, connection_source = (
                validated_step.run,
                validated_step.connection_source,
            )
        except Exception as e:
            log.error(
                "Failed to render or validate step parameters.",
                error=str(e),
                exc_info=True,
            )
            raise ValueError(
                f"Failed to render parameters for step '{step.name}': {e}"
            ) from e
        if action.action.startswith("browser_"):
            if not active_session or not stateful_strategy:
                raise RuntimeError("Browser action called without active session.")
            command_info = {
                "command_type": action.action.replace("browser_", "", 1),
                "name": validated_step.name,
                "text": getattr(action, "text", getattr(action, "url", None)),
                "element_info": {
                    "locators": {"css_selector": getattr(action, "target", None)}
                },
            }
            step_index = (
                int("".join(filter(str.isdigit, validated_step.id)))
                if any(char.isdigit() for char in validated_step.id)
                else 0
            )
            return await stateful_strategy.execute_step(
                active_session, command_info, step_index
            )
        connection, secrets, strategy = None, None, None
        if connection_source:
            connection, secrets = await self.resolver.resolve(connection_source)
            strategy = self.connector._get_strategy_for_connection_model(connection)
        if (
            stateful_strategy
            and strategy
            and strategy.strategy_key == stateful_strategy.strategy_key
        ):
            return {
                "status": "success",
                "message": f"Session setup step '{validated_step.name}' completed.",
            }
        if action.action == "run_transform":
            transformer_run_context = {
                "initial_input": action.input_data.get("data", []),
                "query_parameters": action.input_data.get("query_parameters", {}),
            }
            transformer_script_path = resolve_path(action.script_path)
            with open(transformer_script_path, "r") as f:
                transformer_script_data = yaml.safe_load(f)
            return await TransformerService().run(
                transformer_script_data, transformer_run_context
            )
        if not strategy:
            raise ValueError(
                f"Step '{validated_step.name}' requires a connection_source for a stateless action."
            )

        if action.action == "run_sql_query":
            query_source = action.query
            print("query_source")
            print(query_source)

            query_string = query_source
            if query_source.startswith(("file:", "app-asset:")):
                query_path = resolve_path(query_source)
                print("query_path")
                print(query_path)
                query_string = query_path.read_text(encoding="utf-8")
            return await strategy.execute_query(
                query_string, action.parameters, connection, secrets
            )
        if hasattr(strategy, action.action):
            method_to_call = getattr(strategy, action.action)
            if action.action in [
                "run_python_script",
                "write_files",
                "aggregate_content",
            ]:
                return await method_to_call(
                    connection, action.model_dump(), run_context.get("script_input", {})
                )
            else:
                return await method_to_call(
                    connection,
                    secrets,
                    action.model_dump(),
                    run_context.get("script_input", {}),
                )
        raise NotImplementedError(
            f"Action '{action.action}' is not implemented by the '{strategy.strategy_key}' strategy."
        )

    # async def run_script_model(
    #     self,
    #     script_model: ConnectorScript,
    #     script_input: Dict[str, Any] = None,
    #     session_variables: Dict[str, Any] = None,
    #     no_cache: bool = False,
    # ):
    #     log = logger.bind(script_name=script_model.name, no_cache=no_cache)
    #     if no_cache:
    #         log.info("engine.run.cache_disabled")

    #     log.info("engine.run.begin")
    #     run_id = f"run_{uuid.uuid4().hex[:12]}"
    #     run_dir = RUNS_DIR / run_id
    #     run_dir.mkdir(parents=True)
    #     user_params = script_input or {}
    #     manifest = RunManifest(
    #         run_id=run_id,
    #         flow_id=script_model.name,
    #         status="running",
    #         timestamp_utc=datetime.now(timezone.utc),
    #         parameters=user_params,
    #         steps=[],
    #     )
    #     dag = self._build_dependency_graph(script_model.steps)
    #     topological_generations = list(nx.topological_generations(dag))
    #     run_context = {"script_input": user_params, "steps": {}}
    #     final_results: Dict[str, Any] = {}
    #     try:
    #         for generation in topological_generations:
    #             for step_id in generation:
    #                 step_data = dag.nodes[step_id]["step_data"]
    #                 parent_hashes = {
    #                     pred: run_context["steps"][pred]["output_hash"]
    #                     for pred in dag.predecessors(step_id)
    #                 }
    #                 cache_key = self._calculate_cache_key(step_data, parent_hashes)
    #                 cached_step = None
    #                 if not no_cache:
    #                     cached_step = self._find_cached_step(cache_key)
    #                 if cached_step:
    #                     step_result_obj = cached_step
    #                     step_result_obj.cache_hit = True
    #                     raw_result = (
    #                         json.loads(
    #                             self.cache_manager.read_bytes(cached_step.output_hash)
    #                         )
    #                         if cached_step.output_hash
    #                         else None
    #                     )
    #                 else:
    #                     raw_result = await self._execute_step(
    #                         step_data, run_context, session_variables
    #                     )
    #                     output_hash = self.cache_manager.write_json(raw_result)
    #                     step_result_obj = StepResult(
    #                         step_id=step_id,
    #                         status="completed",
    #                         summary="Completed successfully.",
    #                         cache_key=cache_key,
    #                         cache_hit=False,
    #                         output_hash=output_hash,
    #                     )
    #                 manifest.steps.append(step_result_obj)
    #                 final_results[step_data.name] = raw_result
    #                 run_context["steps"][step_id] = {
    #                     "result": raw_result,
    #                     "outputs": {},
    #                     "output_hash": step_result_obj.output_hash,
    #                 }

    #                 # --- DEFINITIVE FIX for Artifact Manifest Population ---
    #                 # After a step runs, check its raw result for an 'artifacts' dictionary.
    #                 # This is the contract with the TransformerService.
    #                 if isinstance(raw_result, dict) and "artifacts" in raw_result:
    #                     log.debug(
    #                         "engine.artifacts.found",
    #                         step_id=step_id,
    #                         artifacts=raw_result["artifacts"],
    #                     )
    #                     # We convert the file paths into proper Artifact objects.
    #                     for artifact_type, paths in raw_result["artifacts"].items():
    #                         path_list = paths if isinstance(paths, list) else [paths]
    #                         for file_path_str in path_list:
    #                             try:
    #                                 file_path = Path(
    #                                     file_path_str.replace("file://", "")
    #                                 )
    #                                 file_bytes = file_path.read_bytes()
    #                                 content_hash = self.cache_manager.write(file_bytes)
    #                                 artifact_name = file_path.name
    #                                 manifest.artifacts[artifact_name] = Artifact(
    #                                     content_hash=content_hash,
    #                                     mime_type="application/octet-stream",  # Simplified for now
    #                                     size_bytes=file_path.stat().st_size,
    #                                 )
    #                             except Exception as e:
    #                                 logger.warn(
    #                                     "engine.artifact.processing_failed",
    #                                     path=file_path_str,
    #                                     error=str(e),
    #                                 )
    #                 # --- END FIX ---

    #         manifest.status = "completed"
    #         log.info("engine.run.success")
    #         return final_results
    #     except Exception as e:
    #         manifest.status = "failed"
    #         log.error("engine.run.failed", error=str(e), exc_info=True)
    #         failed_step_result = StepResult(
    #             step_id="error",
    #             status="failed",
    #             summary=str(e),
    #             cache_key="",
    #             cache_hit=False,
    #         )
    #         manifest.steps.append(failed_step_result)
    #         final_results["error"] = str(e)
    #         return {**final_results, "error": f"{type(e).__name__}: {e}"}
    #     finally:
    #         manifest_path = run_dir / "manifest.json"
    #         manifest_path.write_text(manifest.model_dump_json(indent=2))
    #         log.info("engine.run.manifest_written", path=str(manifest_path))

    async def run_script_model(
        self,
        script_model: ConnectorScript,
        script_input: Dict[str, Any] = None,
        session_variables: Dict[str, Any] = None,
        no_cache: bool = False,
    ):
        log = logger.bind(script_name=script_model.name, no_cache=no_cache)
        if no_cache:
            log.info("engine.run.cache_disabled")

        log.info("engine.run.begin")
        run_id = f"run_{uuid.uuid4().hex[:12]}"
        run_dir = RUNS_DIR / run_id
        run_dir.mkdir(parents=True)
        user_params = script_input or {}
        manifest = RunManifest(
            run_id=run_id,
            flow_id=script_model.name,
            status="running",
            timestamp_utc=datetime.now(timezone.utc),
            parameters=user_params,
            steps=[],
        )
        dag = self._build_dependency_graph(script_model.steps)
        topological_generations = list(nx.topological_generations(dag))
        run_context = {"script_input": user_params, "steps": {}}
        final_results: Dict[str, Any] = {}
        try:
            for generation in topological_generations:
                for step_id in generation:
                    step_data = dag.nodes[step_id]["step_data"]
                    parent_hashes = {
                        pred: run_context["steps"][pred]["output_hash"]
                        for pred in dag.predecessors(step_id)
                    }
                    cache_key = self._calculate_cache_key(step_data, parent_hashes)
                    cached_step = None
                    if not no_cache:
                        cached_step = self._find_cached_step(cache_key)
                    if cached_step:
                        step_result_obj = cached_step
                        step_result_obj.cache_hit = True
                        raw_result = (
                            json.loads(
                                self.cache_manager.read_bytes(cached_step.output_hash)
                            )
                            if cached_step.output_hash
                            else None
                        )
                    else:
                        raw_result = await self._execute_step(
                            step_data, run_context, session_variables
                        )
                        output_hash = self.cache_manager.write_json(raw_result)
                        step_result_obj = StepResult(
                            step_id=step_id,
                            status="completed",
                            summary="Completed successfully.",
                            cache_key=cache_key,
                            cache_hit=False,
                            output_hash=output_hash,
                        )

                    # --- START OF FIX: Implement output processing ---
                    import jmespath

                    step_outputs = {}
                    if step_data.outputs:
                        log.debug(
                            "engine.outputs.processing",
                            step_id=step_id,
                            outputs=step_data.outputs,
                        )
                        for output_name, jmespath_query in step_data.outputs.items():
                            try:
                                extracted_value = jmespath.search(
                                    jmespath_query, raw_result
                                )
                                step_outputs[output_name] = extracted_value
                                log.debug(
                                    "engine.outputs.extracted",
                                    output_name=output_name,
                                    value=extracted_value,
                                )
                            except Exception as e:
                                log.warn(
                                    "engine.outputs.jmespath_failed",
                                    query=jmespath_query,
                                    error=str(e),
                                )
                                step_outputs[output_name] = None
                    # --- END OF FIX ---

                    manifest.steps.append(step_result_obj)
                    final_results[step_data.name] = raw_result

                    # --- START OF FIX: Update the run_context correctly ---
                    run_context["steps"][step_id] = {
                        "result": raw_result,
                        "outputs": step_outputs,  # <-- Use the extracted outputs here
                        "output_hash": step_result_obj.output_hash,
                    }
                    # --- END OF FIX ---

                    if isinstance(raw_result, dict) and "artifacts" in raw_result:
                        log.debug(
                            "engine.artifacts.found",
                            step_id=step_id,
                            artifacts=raw_result["artifacts"],
                        )
                        for artifact_type, paths in raw_result["artifacts"].items():
                            path_list = paths if isinstance(paths, list) else [paths]
                            for file_path_str in path_list:
                                try:
                                    file_path = Path(
                                        file_path_str.replace("file://", "")
                                    )
                                    file_bytes = file_path.read_bytes()
                                    content_hash = self.cache_manager.write(file_bytes)
                                    artifact_name = file_path.name
                                    manifest.artifacts[artifact_name] = Artifact(
                                        content_hash=content_hash,
                                        mime_type="application/octet-stream",
                                        size_bytes=file_path.stat().st_size,
                                    )
                                except Exception as e:
                                    logger.warn(
                                        "engine.artifact.processing_failed",
                                        path=file_path_str,
                                        error=str(e),
                                    )

            manifest.status = "completed"
            log.info("engine.run.success")
            return final_results
        except Exception as e:
            manifest.status = "failed"
            log.error("engine.run.failed", error=str(e), exc_info=True)
            failed_step_result = StepResult(
                step_id="error",
                status="failed",
                summary=str(e),
                cache_key="",
                cache_hit=False,
            )
            manifest.steps.append(failed_step_result)
            final_results["error"] = str(e)
            return {**final_results, "error": f"{type(e).__name__}: {e}"}
        finally:
            manifest_path = run_dir / "manifest.json"
            manifest_path.write_text(manifest.model_dump_json(indent=2))
            log.info("engine.run.manifest_written", path=str(manifest_path))

    async def run_script(
        self,
        script_path: Path,
        script_input: Dict[str, Any] = None,
        session_variables: Dict[str, Any] = None,
        no_cache: bool = False,
    ):
        log = logger.bind(script_path=str(script_path))
        log.info("engine.load_script.begin")
        with open(script_path, "r", encoding="utf-8") as f:
            script_data = yaml.safe_load(f)
        script_model = ConnectorScript(**script_data)
        return await self.run_script_model(
            script_model, script_input, session_variables, no_cache=no_cache
        )
