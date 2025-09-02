import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List

import httpx
import jmespath
import structlog
import yaml
import networkx as nx


from jinja2 import Environment
from cx_core_schemas.connector_script import ConnectorScript, ConnectorStep
from cx_core_schemas.vfs import VfsFileContentResponse, VfsNodeMetadata

from .caching.manager import CacheManager
from .config import ConnectionResolver
from .utils import get_nested_value, safe_serialize

if TYPE_CHECKING:
    from cx_core_schemas.connection import Connection
    from .service import ConnectorService
logger = structlog.get_logger(__name__)


class ScriptEngine:
    """
    Orchestrates the execution of a declarative .connector.yaml script.

    Its primary responsibilities are to parse the script, inject dynamic context
    from piped input, and dispatch actions to the appropriate connection strategies
    in a stateless manner.
    """

    def __init__(self, resolver: ConnectionResolver, connector: "ConnectorService"):
        """
        Initializes the ScriptEngine with its required dependencies.

        Args:
            resolver: An instance of ConnectionResolver for resolving connection details.
            connector: An instance of ConnectorService for accessing strategies.
        """
        self.resolver = resolver
        self.connector = connector
        self.cache_manager = CacheManager()
        self.jinja_env = Environment()

    def _build_dependency_graph(self, steps: list[ConnectorStep]) -> nx.DiGraph:
        """Parses script steps and builds a NetworkX dependency graph."""
        dag = nx.DiGraph()
        step_map = {step.id: step for step in steps}

        for step in steps:
            dag.add_node(step.id, step_data=step)
            # Add explicit dependencies first
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

    async def _execute_step(
        self, step: ConnectorStep, run_context: Dict, debug_mode: bool
    ) -> Any:
        """Executes a single, fully-rendered step of the workflow."""
        log = logger.bind(step_id=step.id, step_name=step.name)

        # --- Just-in-Time Rendering ---
        def recursive_render(data: Any, context: Dict):
            if isinstance(data, dict):
                return {k: recursive_render(v, context) for k, v in data.items()}
            if isinstance(data, list):
                return [recursive_render(i, context) for i in data]
            if isinstance(data, str) and "{{" in data:
                return self.jinja_env.from_string(data).render(context)
            return data

        try:
            rendered_action_data = recursive_render(step.run.model_dump(), run_context)
            action = type(step.run)(**rendered_action_data)
        except Exception as e:
            log.error(
                "Failed to render action parameters.", error=str(e), exc_info=True
            )
            raise ValueError(
                f"Failed to render action parameters for step '{step.name}': {e}"
            ) from e

        # --- Action Dispatcher ---
        connection, secrets = await self.resolver.resolve(step.connection_source)
        strategy = self.connector._get_strategy_for_connection_model(connection)

        # This is the full, non-placeholder dispatcher logic
        if action.action == "test_connection":
            return await strategy.test_connection(connection, secrets)
        elif action.action == "run_declarative_action":
            return await strategy.run_declarative_action(
                connection,
                secrets,
                action.model_dump(),
                run_context.get("script_input", {}),
                debug_mode,
            )
        elif action.action == "browse_path":
            response = await strategy.browse_path(
                action.path.strip("/").split("/"), connection, secrets
            )
            # For browse, the raw list of nodes is often the desired output.
            # The _transform helper is for older strategies returning raw httpx responses.
            if isinstance(response, httpx.Response):
                return self._transform_browse_response(
                    response, connection, action.path
                )
            return response
        elif action.action == "read_content":
            # We now pass the raw, unsplit path as a single-element list.
            # This preserves the full URL. The strategy is now responsible
            # for correctly interpreting the contents of this list.
            path_parts = [action.path]
            vfs_response = await strategy.get_content(path_parts, connection, secrets)
            # --- END FIX ---
            final_result_for_user = vfs_response.model_dump()
            raw_content_for_cache = vfs_response.content.encode("utf-8")
            # return vfs_response.content
            return final_result_for_user
        elif action.action == "run_sql_query":
            query_source = action.query
            query_string = (
                Path(query_source.split(":", 1)[1]).read_text(encoding="utf-8")
                if query_source.startswith("file:")
                else query_source
            )
            query_data = await strategy.execute_query(
                query_string, action.parameters, connection, secrets
            )
            return {"parameters": action.parameters, "data": query_data}
        elif hasattr(strategy, action.action):
            method_to_call = getattr(strategy, action.action)
            # Pass all required context to the strategy methods
            if action.action in [
                "aggregate_content",
                "run_python_script",
                "write_files",
            ]:
                return await method_to_call(
                    connection, action.model_dump(), run_context.get("script_input", {})
                )
            else:
                return await method_to_call(connection, action.model_dump())

        raise NotImplementedError(
            f"Action '{action.action}' is not implemented or supported by the '{strategy.strategy_key}' strategy."
        )

    async def run_script_model(
        self,
        script_model: ConnectorScript,
        script_input: Dict[str, Any] = {},
        debug_mode: bool = False,
    ):
        """
        Executes a declarative workflow directly from a Pydantic model instance.
        """
        log = logger.bind(script_name=script_model.name)
        log.info("DAG-based ScriptEngine running script from model.")

        dag = self._build_dependency_graph(script_model.steps)
        topological_generations = list(nx.topological_generations(dag))
        run_context = {"script_input": script_input, "steps": {}}
        results: Dict[str, Any] = {}

        for generation in topological_generations:
            tasks = [
                self._execute_step(
                    dag.nodes[step_id]["step_data"], run_context, debug_mode
                )
                for step_id in generation
            ]
            generation_results = await asyncio.gather(*tasks, return_exceptions=True)
            for step_id, step_result in zip(generation, generation_results):
                step_data = dag.nodes[step_id]["step_data"]
                if isinstance(step_result, Exception):
                    log.error("Step failed", step_id=step_id, error=str(step_result))
                    # In interactive mode, we want to see the error but not crash.
                    results[step_data.name] = {
                        "error": f"{type(step_result).__name__}: {step_result}"
                    }
                    # We might choose to stop or continue on error depending on the use case.
                    # For a simple interactive command, stopping is fine.
                    return results

                results[step_data.name] = step_result
                step_outputs = {}
                if step_data.outputs:
                    for output_name, query in step_data.outputs.items():
                        try:
                            step_outputs[output_name] = jmespath.search(
                                query, step_result
                            )
                        except Exception as e:
                            log.warn(
                                "Failed to extract output",
                                output_name=output_name,
                                error=str(e),
                            )
                run_context["steps"][step_id] = {
                    "result": step_result,
                    "outputs": step_outputs,
                }

        log.info("Script execution finished successfully.")
        return results

    async def run_script(
        self,
        script_path: Path,
        script_input: Dict[str, Any] = {},
        debug_mode: bool = False,
    ):
        log = logger.bind(script_path=str(script_path))
        log.info("DAG-based ScriptEngine running script from file.")

        with open(script_path, "r", encoding="utf-8") as f:
            script_data = yaml.safe_load(f)
        script_data["script_input"] = script_input
        script_model = ConnectorScript(**script_data)

        # Delegate the core execution logic to the new model-based runner
        return await self.run_script_model(script_model, script_input, debug_mode)

    # async def run_script(
    #     self,
    #     script_path: Path,
    #     script_input: Dict[str, Any] = {},
    #     debug_mode: bool = False,
    # ):
    #     log = logger.bind(script_path=str(script_path))
    #     log.info("DAG-based ScriptEngine running script.")

    #     with open(script_path, "r", encoding="utf-8") as f:
    #         script_data = yaml.safe_load(f)
    #     script_data["script_input"] = script_input
    #     script_model = ConnectorScript(**script_data)

    #     dag = self._build_dependency_graph(script_model.steps)
    #     topological_generations = list(nx.topological_generations(dag))

    #     run_context = {"script_input": script_input, "steps": {}}
    #     results: Dict[str, Any] = {}

    #     for generation in topological_generations:
    #         tasks = [
    #             self._execute_step(
    #                 dag.nodes[step_id]["step_data"], run_context, debug_mode
    #             )
    #             for step_id in generation
    #         ]
    #         generation_results = await asyncio.gather(*tasks, return_exceptions=True)

    #         for step_id, step_result in zip(generation, generation_results):
    #             step_data = dag.nodes[step_id]["step_data"]
    #             if isinstance(step_result, Exception):
    #                 log.error(
    #                     "Step failed during parallel execution.",
    #                     step_id=step_id,
    #                     error=str(step_result),
    #                     exc_info=step_result,
    #                 )
    #                 results[step_data.name] = {
    #                     "error": f"{type(step_result).__name__}: {step_result}"
    #                 }
    #                 return results  # Fail fast

    #             results[step_data.name] = step_result
    #             step_outputs = {}
    #             if step_data.outputs:
    #                 for output_name, query in step_data.outputs.items():
    #                     try:
    #                         step_outputs[output_name] = jmespath.search(
    #                             query, step_result
    #                         )
    #                     except Exception as e:
    #                         log.warn(
    #                             "Failed to extract output.",
    #                             output_name=output_name,
    #                             error=str(e),
    #                         )
    #             run_context["steps"][step_id] = {
    #                 "result": step_result,
    #                 "outputs": step_outputs,
    #             }

    #     log.info("Script execution finished successfully.")
    #     return results

    def _transform_browse_response(
        self, response: httpx.Response, connection: "Connection", vfs_path: str
    ) -> List[Dict[str, Any]]:
        """
        (Backward Compatibility) Transforms a raw HTTP response from a 'browse'
        action into a VFS node list for strategies that do not format their own output.
        """
        response_data = response.json()
        browse_config = connection.catalog.browse_config if connection.catalog else {}
        config_for_path = next(
            (
                item
                for item in browse_config.get("root", [])
                if item.get("path") == f"{vfs_path.strip('/')}/"
            ),
            {},
        )

        response_key = config_for_path.get("response_key")
        items_list = response_data.get(response_key) if response_key else response_data

        vfs_nodes = []
        if isinstance(items_list, list):
            for item in items_list:
                if not isinstance(item, dict):
                    continue
                item_id = get_nested_value(
                    item, config_for_path.get("item_id_key", "id")
                )
                item_name = (
                    get_nested_value(item, config_for_path.get("item_name_key", "name"))
                    or f"Item #{item_id}"
                )
                if item_id:
                    vfs_nodes.append(
                        {
                            "name": str(item_name),
                            "path": f"{vfs_path.strip('/')}/{item_id}",
                            "type": "file",
                            "icon": config_for_path.get("item_icon", "IconFileInfo"),
                        }
                    )
        return vfs_nodes

    def _transform_content_response(
        self, response: httpx.Response, connection: "Connection", vfs_path: str
    ) -> "VfsFileContentResponse":
        """
        (Backward Compatibility) Transforms a raw HTTP response from a 'read' action
        into a VfsFileContentResponse for strategies that do not format their own output.
        """
        content_data = response.json()
        browse_config = connection.catalog.browse_config if connection.catalog else {}

        response_key_template = browse_config.get("get_content_response_key_template")
        if response_key_template:
            item_type = vfs_path.strip("/").split("/")[0]
            response_key = response_key_template.replace(
                "{{ item_type_singular }}", item_type.rstrip("s")
            )
            content_data = content_data.get(response_key, content_data)

        content_as_string = json.dumps(safe_serialize(content_data), indent=2)
        now = datetime.now(timezone.utc)
        etag = response.headers.get("etag", f'"{hash(content_as_string)}"')

        metadata = VfsNodeMetadata(
            can_write=False, is_versioned=False, etag=etag, last_modified=now
        )
        full_vfs_path = f"vfs://connections/{connection.id}{vfs_path}"

        return VfsFileContentResponse(
            path=full_vfs_path,
            content=content_as_string,
            mime_type="application/json",
            last_modified=now,
            size=len(content_as_string.encode("utf-8")),
            metadata=metadata,
        )
