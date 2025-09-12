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
from ...utils import CX_HOME


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

        # Define and register the 'now' function as a global in the Jinja environment.
        def get_now(tz: str | None = None) -> datetime:
            """A Jinja-friendly wrapper for datetime.now."""
            if tz and tz.lower() == "utc":
                return datetime.now(timezone.utc)
            return datetime.now()

        self.jinja_env.globals["now"] = get_now

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
        active_session: Any = None,
        stateful_strategy: Any = None,
    ) -> Any:
        log = logger.bind(step_id=step.id, step_name=step.name)

        # 1. First, build the base context with global and session variables.
        base_render_context = {
            "CX_HOME": str(CX_HOME.resolve()),
            "steps": run_context.get("steps", {}),
            "script_input": run_context.get("script_input", {}),
            **(session_variables or {}),
        }

        # 2. Extract the step's `run` block as a dictionary.
        run_block_dict = step.run.model_dump()

        # 3. Merge the run block into the base context. This makes keys like
        #    'metadata' directly available to templates within the same run block.
        full_render_context = {**base_render_context, **run_block_dict}

        # 2. Define a robust, recursive rendering function.
        def recursive_render(data: Any, context: Dict):
            if isinstance(data, dict):
                return {k: recursive_render(v, context) for k, v in data.items()}
            if isinstance(data, list):
                return [recursive_render(i, context) for i in data]
            if isinstance(data, str):
                # Heuristic for native type evaluation (e.g., "{{ my_list }}")
                stripped_data = data.strip()
                if (
                    stripped_data.startswith("{{")
                    and stripped_data.endswith("}}")
                    and stripped_data.count("{{") == 1
                ):
                    expression = stripped_data[2:-2].strip()
                    try:
                        compiled_expr = self.jinja_env.compile_expression(expression)
                        return compiled_expr(**context)
                    except Exception:
                        pass  # Fallback to string rendering on error
                # Standard string interpolation
                if "{{" in data:
                    return self.jinja_env.from_string(data).render(**context)
            return data

        # 3. Render the ENTIRE step object and re-validate it.
        # This is the crucial step that ensures templates in fields like
        # `connection_source` and `query` are processed correctly.
        try:
            rendered_step_data = recursive_render(
                step.model_dump(), full_render_context
            )
            validated_step = ConnectorStep(**rendered_step_data)
            action = validated_step.run
            connection_source = validated_step.connection_source
        except Exception as e:
            log.error(
                "Failed to render or validate step parameters.",
                error=str(e),
                exc_info=True,
            )
            raise ValueError(
                f"Failed to render parameters for step '{step.name}': {e}"
            ) from e

        # --- END OF RENDERING LOGIC ---

        # --- Action Dispatcher ---
        # It now uses the fully rendered and validated step data.

        if action.action.startswith("browser_"):
            if not active_session or not stateful_strategy:
                raise RuntimeError(
                    "A browser action was called, but no active browser session was found."
                )
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
            transformer_service = TransformerService()
            return await transformer_service.run(
                transformer_script_data, transformer_run_context
            )

        if not strategy:
            raise ValueError(
                f"Step '{validated_step.name}' requires a connection_source for a stateless action."
            )

        if action.action == "run_sql_query":
            query_source = action.query
            query_string = ""
            if query_source.startswith(("file:", "app-asset:")):
                query_path = resolve_path(query_source.replace("file:", ""))
                query_string = query_path.read_text(encoding="utf-8")
            else:
                query_string = query_source
            return await strategy.execute_query(
                query_string, action.parameters, connection, secrets
            )

        # This handles all other standard actions like run_declarative_action, read_content, etc.
        # It relies on the strategy implementing a method with the same name as the action.
        if hasattr(strategy, action.action):
            method_to_call = getattr(strategy, action.action)

            # --- THIS IS THE DEFINITIVE FIX ---
            # These specific strategies do not require the 'secrets' argument.
            if action.action in [
                "run_python_script",
                "write_files",
                "aggregate_content",
            ]:
                return await method_to_call(
                    connection, action.model_dump(), run_context.get("script_input", {})
                )
            else:
                # All other strategies follow the standard (connection, secrets, ...) signature.
                return await method_to_call(
                    connection,
                    secrets,
                    action.model_dump(),
                    run_context.get("script_input", {}),
                )
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
        Executes a declarative workflow, now with support for stateful session providers
        like the web browser, handling step dependencies, context injection, and error propagation.
        """
        log = logger.bind(script_name=script_model.name)
        log.info("DAG-based ScriptEngine running script from model.")

        dag = self._build_dependency_graph(script_model.steps)
        topological_generations = list(nx.topological_generations(dag))

        processed_script_input = script_input or {}
        processed_session_variables = session_variables or {}

        run_context = {"script_input": processed_script_input, "steps": {}}
        results: Dict[str, Any] = {}

        # --- NEW: Stateful Session Management ---
        active_stateful_session = None
        stateful_strategy = None

        # This new top-level key in the script triggers stateful mode.
        session_provider_key = getattr(script_model, "session_provider", None)

        try:
            # --- Session Initialization (if required) ---
            if session_provider_key:
                log.info(
                    "Stateful session provider detected.", provider=session_provider_key
                )
                # Find the step that defines the connection for this session provider
                setup_step = next(
                    (s for s in script_model.steps if s.connection_source), None
                )
                if not setup_step or not setup_step.connection_source:
                    raise ValueError(
                        f"A flow with a session_provider='{session_provider_key}' requires at least one step with a connection_source."
                    )

                connection, secrets = await self.resolver.resolve(
                    setup_step.connection_source
                )
                strategy_instance = self.connector._get_strategy_for_connection_model(
                    connection
                )

                # Check if the strategy supports the stateful lifecycle methods.
                if not hasattr(strategy_instance, "start_session"):
                    raise TypeError(
                        f"The strategy for '{connection.catalog.connector_provider_key}' is not a stateful session provider."
                    )

                stateful_strategy = strategy_instance
                active_stateful_session = await stateful_strategy.start_session(
                    connection, secrets
                )
                log.info("Stateful session started successfully.")

            # --- Main Execution Loop ---
            for generation in topological_generations:
                tasks = []
                for step_id in generation:
                    step_data = dag.nodes[step_id]["step_data"]
                    # Pass the active session to the execution helper
                    tasks.append(
                        self._execute_step(
                            step_data,
                            run_context,
                            debug_mode,
                            processed_session_variables,
                            active_stateful_session,  # NEW
                            stateful_strategy,  # NEW
                        )
                    )

                generation_results = await asyncio.gather(
                    *tasks, return_exceptions=True
                )

                for step_id, step_result in zip(generation, generation_results):
                    step_data = dag.nodes[step_id]["step_data"]
                    if isinstance(step_result, Exception):
                        log.error(
                            "Step failed during execution.",
                            step_id=step_id,
                            error=str(step_result),
                            exc_info=step_result if debug_mode else False,
                        )
                        results[step_data.name] = {
                            "error": f"{type(step_result).__name__}: {step_result}"
                        }
                        # On failure, immediately exit the loop and proceed to the 'finally' block for cleanup.
                        raise step_result

                    results[step_data.name] = step_result
                    step_outputs = {}
                    if step_data.outputs:
                        for output_name, query in step_data.outputs.items():
                            try:
                                step_outputs[output_name] = jmespath.search(
                                    query, step_result
                                )
                            except Exception as e:
                                log.warning(
                                    "Failed to extract output.",
                                    step_name=step_data.name,
                                    output_name=output_name,
                                    error=str(e),
                                )

                    run_context["steps"][step_id] = {
                        "result": step_result,
                        "outputs": step_outputs,
                    }

            log.info("Script execution finished successfully.")
            return results

        except Exception as e:
            # This block now catches failures from the execution loop to ensure cleanup happens.
            log.error("Script execution failed.", error=str(e), exc_info=True)
            # We don't re-raise immediately; we let the finally block run first.
            # Return the partial results which will contain the error message.
            return results

        finally:
            # --- Session Teardown (Guaranteed to run) ---
            if active_stateful_session and stateful_strategy:
                log.info("Cleaning up stateful session...")
                await stateful_strategy.end_session()
                log.info("Stateful session cleaned up successfully.")

    async def run_script(
        self,
        script_path: Path,
        script_input: Dict[str, Any] = {},
        debug_mode: bool = False,
        session_variables: Dict[str, Any] = None,
    ):
        log = logger.bind(script_path=str(script_path))
        log.info("DAG-based ScriptEngine running script from file.")

        with open(script_path, "r", encoding="utf-8") as f:
            script_data = yaml.safe_load(f)
        script_data["script_input"] = script_input
        script_model = ConnectorScript(**script_data)

        # Delegate the core execution logic to the new model-based runner
        return await self.run_script_model(
            script_model, script_input, debug_mode, session_variables
        )

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
