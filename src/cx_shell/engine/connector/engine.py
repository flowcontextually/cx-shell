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
from ...engine.transformer.service import TransformerService
from ...utils import resolve_path

from .caching.manager import CacheManager
from .config import ConnectionResolver
from .utils import get_nested_value, safe_serialize

if TYPE_CHECKING:
    from cx_core_schemas.connection import Connection
    from .service import ConnectorService
logger = structlog.get_logger(__name__)


def sql_quote_filter(value):
    """A Jinja2 filter that correctly quotes a value for use in a SQL query."""
    if value is None:
        return "NULL"
    # Basic escaping for single quotes within the string
    sanitized_value = str(value).replace("'", "''")
    return f"'{sanitized_value}'"


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
        self.jinja_env.filters["sqlquote"] = sql_quote_filter

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
        self,
        step: ConnectorStep,
        run_context: Dict,
        debug_mode: bool,
        session_variables: Dict[str, Any] = None,
    ) -> Any:
        log = logger.bind(step_id=step.id, step_name=step.name)
        session_vars = session_variables or {}

        def recursive_render(data: Any, context: Dict):
            """
            Recursively renders Jinja templates.
            Crucially, if a string is JUST a single Jinja block, it evaluates
            it to its native Python type instead of casting to a string.
            """
            if isinstance(data, dict):
                return {k: recursive_render(v, context) for k, v in data.items()}
            if isinstance(data, list):
                return [recursive_render(i, context) for i in data]

            if isinstance(data, str):
                stripped_data = data.strip()
                # Heuristic: if the string is ONLY a Jinja block, evaluate it.
                if (
                    stripped_data.startswith("{{")
                    and stripped_data.endswith("}}")
                    and stripped_data.count("{{") == 1
                ):
                    expression = stripped_data[2:-2].strip()
                    try:
                        # Compile and run the expression to get the native object
                        compiled_expr = self.jinja_env.compile_expression(expression)
                        return compiled_expr(**context)
                    except Exception:
                        # Fallback to string rendering on error
                        pass

                # For interpolation ("Hello {{ name }}") or fallbacks, render as string.
                if "{{" in data:
                    return self.jinja_env.from_string(data).render(**context)

            return data

        try:
            full_render_context = {**run_context, **session_vars}
            rendered_action_data = recursive_render(
                step.run.model_dump(), full_render_context
            )
            # log.info(
            #     "DEBUG: Data rendered by Jinja before Pydantic validation",
            #     data=safe_serialize(rendered_action_data),
            # )
            action = type(step.run)(**rendered_action_data)
        except Exception as e:
            log.error(
                "Failed to render action parameters.", error=str(e), exc_info=True
            )
            raise ValueError(
                f"Failed to render action parameters for step '{step.name}': {e}"
            ) from e

        # --- Action Dispatcher ---
        connection, secrets, strategy = None, None, None
        if step.connection_source:
            connection, secrets = await self.resolver.resolve(step.connection_source)
            strategy = self.connector._get_strategy_for_connection_model(connection)

        # --- v0.2.1: Native run_transform action ---
        if action.action == "run_transform":
            log.info(
                "Executing native transform action.", script_path=action.script_path
            )
            transformer_run_context = {
                "initial_input": action.input_data.get("data", []),
                "query_parameters": action.input_data.get("query_parameters", {}),
            }
            transformer_script_path = resolve_path(action.script_path)
            with open(transformer_script_path, "r") as f:
                transformer_script_data = yaml.safe_load(f)
            transformer_service = TransformerService()
            return await transformer_service.run(
                transformer_script_data, transformer_run_context
            )

        # --- Standard Connection-Based Actions ---
        if not strategy:
            raise ValueError(
                f"Step '{step.name}' requires a connection_source, but none was provided or resolved."
            )

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
            if isinstance(response, httpx.Response):
                return self._transform_browse_response(
                    response, connection, action.path
                )
            return response
        elif action.action == "read_content":
            path_parts = [action.path]
            vfs_response = await strategy.get_content(path_parts, connection, secrets)
            final_result_for_user = vfs_response.model_dump()
            return final_result_for_user
        if action.action == "run_sql_query":
            query_source = action.query
            query_string = ""
            # Check for our special URI schemes
            if query_source.startswith(("file:", "app-asset:")):
                # Use our universal path resolver
                query_path = resolve_path(query_source.replace("file:", ""))
                query_string = query_path.read_text(encoding="utf-8")
            else:
                # If no protocol, it's a raw SQL string
                query_string = query_source

            # Now, call the strategy with the resolved SQL content
            query_data = await strategy.execute_query(
                query_string, action.parameters, connection, secrets
            )
            return {"parameters": action.parameters, "data": query_data}
        elif hasattr(strategy, action.action):
            method_to_call = getattr(strategy, action.action)
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
        script_input: Dict[str, Any] = None,
        debug_mode: bool = False,
        session_variables: Dict[str, Any] = None,
    ):
        """
        Executes a declarative workflow directly from a Pydantic model instance,
        handling step dependencies, context injection, and error propagation.

        Args:
            script_model: The Pydantic model of the script to execute.
            script_input: Data piped from a previous command or stdin.
            debug_mode: Flag to enable verbose debugging features.
            session_variables: A dictionary of variables from the interactive session
                               to be made available for templating.
        """
        log = logger.bind(script_name=script_model.name)
        log.info("DAG-based ScriptEngine running script from model.")

        dag = self._build_dependency_graph(script_model.steps)
        topological_generations = list(nx.topological_generations(dag))

        # Ensure script_input and session_variables are dictionaries to prevent None errors.
        processed_script_input = script_input or {}
        processed_session_variables = session_variables or {}

        run_context = {"script_input": processed_script_input, "steps": {}}
        results: Dict[str, Any] = {}

        for generation in topological_generations:
            tasks = [
                self._execute_step(
                    dag.nodes[step_id]["step_data"],
                    run_context,
                    debug_mode,
                    processed_session_variables,
                )
                for step_id in generation
            ]

            # Execute steps in the current generation concurrently
            generation_results = await asyncio.gather(*tasks, return_exceptions=True)

            for step_id, step_result in zip(generation, generation_results):
                step_data = dag.nodes[step_id]["step_data"]

                # If a step failed, capture the error and halt execution of subsequent steps.
                if isinstance(step_result, Exception):
                    log.error(
                        "Step failed during execution.",
                        step_id=step_id,
                        step_name=step_data.name,
                        error=str(step_result),
                        exc_info=step_result if debug_mode else False,
                    )
                    # Package the error into a clean dictionary for the caller
                    results[step_data.name] = {
                        "error": f"{type(step_result).__name__}: {step_result}"
                    }
                    # Immediately return to prevent dependent steps from running
                    return results

                # If the step succeeded, store its result
                results[step_data.name] = step_result
                step_outputs = {}
                if step_data.outputs:
                    for output_name, query in step_data.outputs.items():
                        try:
                            # Use JMESPath to extract specific values from the result
                            step_outputs[output_name] = jmespath.search(
                                query, step_result
                            )
                        except Exception as e:
                            log.warn(
                                "Failed to extract output from step result.",
                                step_name=step_data.name,
                                output_name=output_name,
                                jmespath_query=query,
                                error=str(e),
                            )

                # Update the run_context so subsequent steps can reference this step's output
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
