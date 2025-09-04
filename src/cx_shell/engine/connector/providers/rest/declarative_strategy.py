import base64
import hashlib
import json
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Dict, List, TYPE_CHECKING
from pydantic import TypeAdapter, ValidationError
import httpx
import structlog
from jinja2 import Environment, TemplateError
import importlib.util


# --- Conditional Imports for Type Hinting ---
if TYPE_CHECKING:
    from cx_core_schemas.connection import Connection
    from ...vfs_reader import AbstractVfsReader
    from cx_core_schemas.api_catalog import ApiCatalog


from ...utils import safe_serialize
from ..base import BaseConnectorStrategy
from .....utils import resolve_path
from cx_core_schemas.vfs import VfsFileContentResponse, VfsNodeMetadata


logger = structlog.get_logger(__name__)


def get_nested_value(data: Dict, key_path: str, default: Any = None) -> Any:
    """Safely retrieves a value from a nested dictionary using dot notation."""
    if not isinstance(data, dict) or not isinstance(key_path, str):
        return default
    keys = key_path.split(".")
    value = data
    for key in keys:
        if isinstance(value, dict):
            value = value.get(key, default)
        else:
            return default  # Cannot traverse further
    return value


class DeclarativeRestStrategy(BaseConnectorStrategy):
    """
    A pure "engine" that browses and interacts with a REST API based entirely
    on a declarative blueprint. It can create its own authenticated client or use
    one provided by another strategy (like OAuth).
    """

    strategy_key = "rest-declarative"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.vfs_reader: "AbstractVfsReader" | None = kwargs.get("vfs_reader")
        self.jinja_env = Environment(
            autoescape=False, trim_blocks=True, lstrip_blocks=True
        )

        def sha256_hex_filter(s: str) -> str:
            return hashlib.sha256(s.encode()).hexdigest()

        def b64decode_filter(s: str) -> bytes:
            """Decodes a Base64 string into bytes."""
            return base64.b64decode(s)

        self.jinja_env.filters["rstrip"] = lambda s, suffix: s.rstrip(suffix)
        self.jinja_env.filters["sha256_hex"] = sha256_hex_filter
        self.jinja_env.filters["b64decode"] = b64decode_filter

    def _render_template(self, data: Any, context: Dict) -> Any:
        """
        Recursively renders Jinja templates within a data structure.
        Crucially, if a string is JUST a single Jinja block (e.g., "{{ my_list }}"),
        it evaluates it to its native Python type instead of casting to a string.
        """
        if isinstance(data, dict):
            return {k: self._render_template(v, context) for k, v in data.items()}
        if isinstance(data, list):
            return [self._render_template(i, context) for i in data]

        if isinstance(data, str):
            stripped_data = data.strip()
            # HEURISTIC: If the string is ONLY a Jinja block, evaluate the expression
            # to get the native Python object (e.g., a list, a dict).
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
                    # Fallback to standard string rendering on any error
                    pass

            # For interpolation ("Hello {{ name }}") or fallbacks, render as a string.
            if "{{" in data:
                try:
                    template = self.jinja_env.from_string(data)
                    return template.render(context)
                except TemplateError as e:
                    raise ValueError(
                        f"Jinja2 rendering failed for template '{data}': {e}"
                    ) from e

        # For all other types (int, bool, etc.), return as is
        return data

    def _load_pydantic_model(self, model_path_str: str, catalog: "ApiCatalog"):
        schemas_py_file = catalog.schemas_module_path
        if not schemas_py_file or not model_path_str.startswith("schemas."):
            raise ImportError(
                f"Cannot load model '{model_path_str}'. Invalid path or schemas file not found."
            )

        class_name = model_path_str.split(".", 1)[1]
        spec = importlib.util.spec_from_file_location("schemas", schemas_py_file)
        schemas_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(schemas_module)

        return getattr(schemas_module, class_name)

    def _process_directives(self, data: Any) -> Any:
        """
        Recursively processes special 'directive' strings for file handling.
        """
        if isinstance(data, dict):
            return {k: self._process_directives(v) for k, v in data.items()}
        if isinstance(data, list):
            return [self._process_directives(i) for i in data]

        if isinstance(data, str):
            if data.startswith("read_file:"):
                path_str = data.split(":", 1)[1]
                path = resolve_path(path_str.removeprefix("file://"))
                return path.read_text(encoding="utf-8")
            if data.startswith("b64encode_file:"):
                path_str = data.split(":", 1)[1]
                path = resolve_path(path_str.removeprefix("file://"))
                content_bytes = path.read_bytes()
                return base64.b64encode(content_bytes).decode("utf-8")

        return data

    @asynccontextmanager
    async def get_client(self, connection: "Connection", secrets: Dict[str, Any]):
        log = logger.bind(connection_id=connection.id, connection_name=connection.name)
        if not connection.catalog:
            raise ValueError(f"Connection '{connection.name}' is missing catalog data.")

        auth_config = connection.catalog.auth_config or {}
        browse_config = connection.catalog.browse_config or {}
        render_context = {"details": connection.details, "secrets": secrets}

        base_url_template = browse_config.get("base_url_template")
        if not base_url_template:
            raise ValueError("`base_url_template` is missing from browse_config.")
        base_url = self._render_template(base_url_template, render_context)

        auth = None
        headers = {"Content-Type": "application/json"}
        auth_type = auth_config.get("type")

        if auth_type == "basic":
            username = self._render_template(
                auth_config.get("username_template", ""), render_context
            )
            password = self._render_template(
                auth_config.get("password_template", ""), render_context
            )
            auth = httpx.BasicAuth(username=username, password=password)
        elif auth_type == "header":
            header_name = auth_config.get("header_name")
            value_template = auth_config.get("value_template")
            if header_name and value_template:
                headers[header_name] = self._render_template(
                    value_template, render_context
                )

        for header_conf in auth_config.get("additional_headers", []):
            header_name = header_conf.get("name")
            value_template = header_conf.get("value_template")
            if header_name and value_template:
                headers[header_name] = self._render_template(
                    value_template, render_context
                )

        log.info(
            "Final HTTP client headers constructed.",
            base_url=base_url,
            final_headers=headers,
        )

        async with httpx.AsyncClient(
            base_url=base_url, auth=auth, headers=headers, timeout=30.0
        ) as client:
            yield client

    async def test_connection(
        self, connection: "Connection", secrets: Dict[str, Any]
    ) -> bool:
        log = logger.bind(connection_id=connection.id, connection_name=connection.name)
        if not connection.catalog or not connection.catalog.test_connection_config:
            log.warning(
                "test_connection.skip",
                reason="No 'test_connection_config' defined in ApiCatalog.",
            )
            return True
        test_endpoint = connection.catalog.test_connection_config.get("api_endpoint")
        if not test_endpoint:
            log.warning(
                "test_connection.skip",
                reason="'api_endpoint' missing from test_connection_config.",
            )
            return True
        log.info("test_connection.begin", test_endpoint=test_endpoint)
        try:
            async with self.get_client(connection, secrets) as client:
                response = await client.get(test_endpoint)
                response.raise_for_status()
            log.info("test_connection.success", status_code=response.status_code)
            return True
        except httpx.HTTPStatusError as e:
            log.error(
                "test_connection.http_error",
                status_code=e.response.status_code,
                response_text=e.response.text[:500],
            )
            raise ConnectionError(
                f"API returned status {e.response.status_code}"
            ) from e
        except Exception as e:
            log.error("test_connection.unexpected_error", error=str(e), exc_info=True)
            raise ConnectionError(f"An unexpected error occurred: {e}") from e

    @asynccontextmanager
    async def _get_client_manager(
        self,
        connection: "Connection",
        secrets: Dict[str, Any],
        client: httpx.AsyncClient | None = None,
    ):
        """
        An internal context manager that either uses a provided client
        or creates a new one, ensuring a consistent interface for API calls.
        """
        if client:
            # If an authenticated client is passed in (e.g., from an OAuth strategy),
            # simply yield it. No need to create a new context.
            yield client
        else:
            # If no client is provided, create one using our standard method.
            # This ensures connection pooling and headers are handled correctly.
            async with self.get_client(connection, secrets) as new_client:
                yield new_client

    async def browse_path(
        self,
        path_parts: List[str],
        connection: "Connection",
        secrets: Dict[str, Any],
        client: httpx.AsyncClient | None = None,
    ) -> List[Dict[str, Any]]:
        """
        Reads the `browse_config` blueprint and executes API calls to list VFS nodes,
        with support for nested key parsing and automatic pagination.
        """
        # The full, unsplit path is the first and only element.
        full_vfs_path_segment = path_parts[0]
        # Split it here, INSIDE the strategy, where the logic belongs.
        vfs_path_parts = [
            part for part in full_vfs_path_segment.strip("/").split("/") if part
        ]
        # --- END FIX ---

        log = logger.bind(connection_id=connection.id, vfs_path=full_vfs_path_segment)
        browse_config = connection.catalog.browse_config if connection.catalog else None
        if not browse_config:
            return []

        if not path_parts or (len(path_parts) == 1 and not path_parts[0]):
            return [dict(item) for item in browse_config.get("root", [])]

        if len(path_parts) == 1:
            current_level_key = path_parts[0]
            config_for_path = next(
                (
                    item
                    for item in browse_config.get("root", [])
                    if item.get("path") == f"{current_level_key}/"
                ),
                None,
            )
            if not config_for_path or not config_for_path.get("api_endpoint"):
                log.warning("browse_path.no_config_found", path_part=current_level_key)
                return []

            # --- PAGINATION LOGIC ---
            vfs_nodes: List[Dict[str, Any]] = []
            next_url: str | None = config_for_path["api_endpoint"]

            pagination_config = config_for_path.get("pagination_config", {})
            pagination_strategy = pagination_config.get("strategy")
            max_pages = pagination_config.get("max_pages", 1)
            page_count = 0

            log.info(
                "browse_path.begin",
                pagination_strategy=pagination_strategy,
                max_pages=max_pages,
            )

            try:
                async with self._get_client_manager(
                    connection, secrets, client
                ) as active_client:
                    while next_url and page_count < max_pages:
                        page_count += 1
                        log.info(
                            "browse_path.fetching_page",
                            page_number=page_count,
                            url=next_url,
                        )

                        response = await active_client.get(next_url)
                        log.info(
                            "browse_path.api_response", status_code=response.status_code
                        )
                        response.raise_for_status()
                        response_data = response.json()

                        response_key = config_for_path.get("response_key")
                        items_list = (
                            response_data.get(response_key)
                            if response_key
                            else response_data
                        )

                        if not isinstance(items_list, list):
                            log.warning(
                                "browse_path.response_not_a_list",
                                response_key=response_key,
                                response_type=type(items_list).__name__,
                            )
                            break

                        for item in items_list:
                            if not isinstance(item, dict):
                                continue
                            item_id = get_nested_value(
                                item, config_for_path["item_id_key"]
                            )
                            item_name = (
                                get_nested_value(item, config_for_path["item_name_key"])
                                or f"Item #{item_id}"
                            )
                            if item_id:
                                vfs_nodes.append(
                                    {
                                        "name": str(item_name),
                                        "path": f"{current_level_key}/{item_id}",
                                        "type": "file",
                                        "icon": config_for_path.get(
                                            "item_icon", "IconFileInfo"
                                        ),
                                    }
                                )

                        if pagination_strategy == "next_url":
                            next_url = response_data.get(
                                pagination_config.get("next_url_key")
                            )
                        else:
                            next_url = None

                log.info(
                    "browse_path.finished_pagination",
                    total_items_found=len(vfs_nodes),
                    pages_fetched=page_count,
                )
                return vfs_nodes
            except httpx.HTTPStatusError as e:
                log.error(
                    "browse_path.http_error",
                    status_code=e.response.status_code,
                    response_text=e.response.text[:500],
                )
                return []
            except Exception as e:
                log.error("browse_path.unexpected_error", error=str(e), exc_info=True)
                return []

        log.warning("browse_path.unsupported_depth", depth=len(path_parts))
        return []

    async def get_content(
        self,
        path_parts: List[str],
        connection: "Connection",
        secrets: Dict[str, Any],
        client: httpx.AsyncClient | None = None,
    ) -> "VfsFileContentResponse":
        """Fetches content, intelligently handling both relative paths and absolute URLs."""
        # This is the path provided by the user/script, e.g., ['/users/123'] or ['https://...']
        path_segment = path_parts[0] if path_parts else ""

        log = logger.bind(
            connection_id=connection.id, vfs_path=f"/{path_segment.lstrip('/')}"
        )

        # --- THIS IS THE FIX ---
        is_absolute_url = path_segment.lower().startswith(("http://", "https://"))

        if is_absolute_url:
            # If the path is a full URL, use it directly and ignore the base_url.
            endpoint = path_segment
            # Use a generic httpx client for this one-off request.
            # We don't use the connection's configured client because the host is different.
            active_client = httpx.AsyncClient(timeout=30.0)
            log.info("get_content.absolute_url_detected", url=endpoint)
        else:
            # If it's a relative path, use the standard logic with the connection's client.
            browse_config = (
                connection.catalog.browse_config if connection.catalog else None
            )
            if not browse_config:
                raise FileNotFoundError("Browse configuration is missing.")
            render_context = {"details": connection.details, "secrets": secrets}
            endpoint, _ = self._determine_content_endpoint(
                path_parts, browse_config, render_context
            )
            # This is a placeholder for the real client manager logic
            active_client = client or self.get_client(connection, secrets)
        # --- END FIX ---

        log = log.bind(api_endpoint=endpoint)
        log.info("get_content.calling_api")

        try:
            # The 'async with' ensures the client is closed, whether it's a new one
            # for an absolute URL or a managed one from the connection.
            async with active_client as managed_client:
                response = await managed_client.get(endpoint)
                log.info("get_content.api_response", status_code=response.status_code)
                response.raise_for_status()
                content_data = response.json()

            # The rest of the logic for creating the VfsFileContentResponse is the same
            content_as_string = json.dumps(safe_serialize(content_data), indent=2)
            now = datetime.now(timezone.utc)
            etag = response.headers.get("etag", f'"{hash(content_as_string)}"')
            metadata = VfsNodeMetadata(
                can_write=False, is_versioned=False, etag=etag, last_modified=now
            )
            full_vfs_path = (
                f"vfs://connections/{connection.id}/{path_segment.lstrip('/')}"
            )

            return VfsFileContentResponse(
                path=full_vfs_path,
                content=content_as_string,
                mime_type="application/json",
                last_modified=now,
                size=len(content_as_string.encode("utf-8")),
                metadata=metadata,
            )
        except httpx.HTTPStatusError as e:
            log.error(
                "get_content.http_error",
                status_code=e.response.status_code,
                response_text=e.response.text[:500],
            )
            raise FileNotFoundError(
                f"API call to '{endpoint}' failed with status {e.response.status_code}"
            ) from e
        except Exception as e:
            log.error("get_content.unexpected_error", error=str(e), exc_info=True)
            raise IOError(
                f"An unexpected error occurred while fetching content from '{endpoint}': {e}"
            ) from e

    def _determine_content_endpoint(
        self, path_parts: List[str], browse_config: Dict, render_context: Dict
    ) -> tuple[str, str]:
        """
        Determines the final API endpoint to call based on the provided path parts.
        """
        # --- Attempt 1: Structured, Template-Based Resolution ---
        if len(path_parts) == 2:
            item_type_plural, item_id = path_parts[0], path_parts[1]
            template = browse_config.get("get_content_endpoint_template")
            if template:
                render_context["item_type"] = item_type_plural
                render_context["item_id"] = item_id
                endpoint = self._render_template(template, render_context)
                logger.info(
                    "Resolved endpoint using 'get_content_endpoint_template'.",
                    endpoint=endpoint,
                )
                return endpoint, item_type_plural

        if len(path_parts) == 1:
            # This logic is more for VFS-like browsing and less for direct fetches.
            pass

        # --- THIS IS THE DEFINITIVE FIX ---
        # --- Attempt 2: Fallback to Literal Path ---

        # 1. Join the path parts together.
        endpoint = "/".join(path_parts)

        # 2. Ensure the final path starts with exactly one slash.
        # This handles cases where the input might be 'v2/swagger.json' or '/v2/swagger.json'
        # and prevents the double-slash error.
        endpoint = "/" + endpoint.lstrip("/")

        item_type = path_parts[0].lstrip("/") if path_parts else "resource"

        logger.info(
            "No specific content template found in blueprint. Using literal path as endpoint.",
            endpoint=endpoint,
        )
        return endpoint, item_type
        # --- END FIX ---

    async def run_declarative_action(
        self,
        connection: "Connection",
        secrets: Dict[str, Any],
        action_params: Dict[str, Any],
        script_input: Dict[str, Any],
        debug_mode: bool = False,
    ) -> Dict[str, Any]:
        """
        Executes a declarative action, using the Pydantic schema to validate a
        structured context provided by the user, with special handling for file directives.
        """
        template_key = action_params.get("template_key")
        log = logger.bind(connection_id=connection.id, template_key=template_key)

        if not connection.catalog or not connection.catalog.browse_config:
            raise ValueError(
                "Missing ApiCatalog or browse_config for declarative action."
            )

        action_templates = connection.catalog.browse_config.get("action_templates", {})
        template_config = action_templates.get(template_key)
        if not template_config:
            raise ValueError(f"Action '{template_key}' not found in blueprint.")

        user_context = action_params.get("context", {})
        full_render_context = {
            "details": connection.details,
            "secrets": secrets,
            "script_input": script_input,
            "context": user_context,
        }

        api_endpoint = self._render_template(
            template_config.get("api_endpoint", ""), full_render_context
        )
        http_method = template_config.get("http_method", "GET").upper()

        async with self.get_client(connection, secrets) as client:
            request_kwargs = {}
            if (
                http_method in ["POST", "PUT", "PATCH"]
                and "payload_constructor" in template_config
            ):
                constructor_config = template_config["payload_constructor"]
                if "_model" not in constructor_config:
                    raise ValueError(
                        "payload_constructor in blueprint must contain a '_model' key."
                    )

                model_name = constructor_config["_model"]
                PayloadModel = self._load_pydantic_model(model_name, connection.catalog)

                # 1. Render the user's structured context to resolve Jinja variables.
                rendered_context = self._render_template(
                    user_context, full_render_context
                )

                # 2. Recursively process special file-handling directives.
                processed_context = self._process_directives(rendered_context)

                # 3. Validate the final, processed data against the target Pydantic model.
                try:
                    adapter = TypeAdapter(PayloadModel)
                    validated_model = adapter.validate_python(processed_context)
                    request_kwargs["json"] = validated_model.model_dump(
                        by_alias=True, exclude_unset=True
                    )
                    log.info("Successfully validated API payload.", model=model_name)
                except ValidationError as e:
                    log.error(
                        "Payload validation failed against Pydantic model.",
                        model=model_name,
                        errors=str(e),
                    )
                    raise ValueError(
                        f"Failed to build valid payload for {model_name}: {e}"
                    ) from e

            log.info(
                "Executing declarative action API call.",
                endpoint=api_endpoint,
                method=http_method,
            )
            response = await client.request(http_method, api_endpoint, **request_kwargs)
            response.raise_for_status()
            return (
                response.json()
                if response.content
                else {"status_code": response.status_code, "content": None}
            )
