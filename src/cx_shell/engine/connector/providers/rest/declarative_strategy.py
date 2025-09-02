import base64
import hashlib
import json
from contextlib import asynccontextmanager
from datetime import datetime, timezone
import time
from typing import Any, Dict, List, TYPE_CHECKING

import httpx
import structlog
from jinja2 import Environment, TemplateError
from rich.console import Console
from rich.syntax import Syntax


# --- Conditional Imports for Type Hinting ---
if TYPE_CHECKING:
    from cx_core_schemas.connection import Connection
    from ...vfs_reader import AbstractVfsReader

from ...utils import safe_serialize
from ..base import BaseConnectorStrategy
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

    def _render_template(self, template_string: str, context: Dict) -> str:
        """
        Renders a template string using the Jinja2 engine.

        This method is the single point of truth for all templating in the
        declarative REST strategy. It expects a valid Jinja2 template string
        and a context dictionary, and returns the rendered string.
        """
        # If the input is not a string or doesn't contain a template marker,
        # return it as is. This is a fast path for static values.
        if not isinstance(template_string, str) or "{{" not in template_string:
            return template_string

        try:
            # Create a full context that includes the original context PLUS
            # our dynamic 'system' object for things like timestamps.
            full_context = {**context, "system": {"timestamp": str(int(time.time()))}}

            template = self.jinja_env.from_string(template_string)
            return template.render(full_context)

        except TemplateError as e:
            # Provide a rich error message for easier debugging of blueprints.
            raise ValueError(
                f"Jinja2 rendering failed for template '{template_string}': {e}"
            ) from e

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
        """Fetches content for a VFS "file" by interpreting the `browse_config` blueprint."""
        log = logger.bind(
            connection_id=connection.id, vfs_path=f"/{'/'.join(path_parts)}"
        )
        browse_config = connection.catalog.browse_config if connection.catalog else None
        if not browse_config:
            raise FileNotFoundError(
                "Browse configuration is missing from the connection's catalog."
            )

        render_context = {"details": connection.details, "secrets": secrets}
        endpoint, item_type = self._determine_content_endpoint(
            path_parts, browse_config, render_context
        )
        log = log.bind(api_endpoint=endpoint)
        log.info("get_content.calling_api")

        try:
            async with self._get_client_manager(
                connection, secrets, client
            ) as active_client:
                response = await active_client.get(endpoint)
                log.info("get_content.api_response", status_code=response.status_code)
                response.raise_for_status()
                content_data = response.json()

            response_key_template = browse_config.get(
                "get_content_response_key_template"
            )
            if response_key_template:
                render_context = {"item_type": item_type}
                response_key = self._render_template(
                    response_key_template.replace(
                        "item_type_singular", "item_type|rstrip('s')"
                    ),
                    render_context,
                )
                content_data = content_data.get(response_key, content_data)

            content_as_string = json.dumps(safe_serialize(content_data), indent=2)
            now = datetime.now(timezone.utc)
            etag = response.headers.get("etag", f'"{hash(content_as_string)}"')
            metadata = VfsNodeMetadata(
                can_write=False, is_versioned=False, etag=etag, last_modified=now
            )
            full_vfs_path = f"vfs://connections/{connection.id}/{'/'.join(path_parts)}"

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
                f"API call failed with status {e.response.status_code}"
            ) from e
        except Exception as e:
            log.error("get_content.unexpected_error", error=str(e), exc_info=True)
            raise IOError(
                f"An unexpected error occurred while fetching content: {e}"
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
        Executes a generic, templated action defined in the ApiCatalog blueprint.

        This method is the runtime engine for blueprint actions. It looks up the
        action by its key, renders any templates for the URL path and body,
        and makes the appropriate HTTP request.
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
            raise ValueError(
                f"Template key '{template_key}' not found in ApiCatalog blueprint."
            )

        api_endpoint_template = template_config.get("api_endpoint")
        http_method = template_config.get("http_method", "GET").upper()

        if not api_endpoint_template:
            raise ValueError("Action template is missing required key 'api_endpoint'.")

        # The full render context makes secrets, connection details, and action-specific
        # arguments available to the Jinja2 templates.
        # We merge the action's context directly into the top level, so that a
        # template like `/pets/{petId}` can be rendered from an argument `petId=1`.
        action_context = action_params.get("context", {})
        render_context = {
            "details": connection.details,
            "secrets": secrets,
            "script_input": script_input,
            **action_context,  # Unpack the action arguments into the top level
        }
        # Render the API endpoint. This is crucial for substituting path parameters
        # like `/pets/{action.petId}`.
        # Render the API endpoint. This is crucial for substituting path parameters.
        api_endpoint = self._render_template(api_endpoint_template, render_context)
        log.info(
            "Executing declarative action.", endpoint=api_endpoint, method=http_method
        )

        async with self.get_client(connection, secrets) as client:
            request_kwargs = {}
            payload = None

            # Only process a request body if the method is one that typically has one.
            if http_method in ["POST", "PUT", "PATCH"]:
                payload_template_str = template_config.get("payload_template")
                if payload_template_str:
                    # Special functions for reading local files can be injected here.
                    # self.jinja_env.globals["read_attachment"] = read_attachment

                    rendered_payload_str = self._render_template(
                        payload_template_str, render_context
                    )

                    if debug_mode:
                        console = Console(stderr=True)
                        console.print(
                            "\n--- [bold yellow]DEBUG: Rendered Payload[/bold yellow] ---"
                        )
                        syntax = Syntax(
                            rendered_payload_str,
                            "json",
                            theme="default",
                            line_numbers=True,
                        )
                        console.print(syntax)
                        console.print("--- [bold yellow]END DEBUG[/bold yellow] ---\n")

                    payload = json.loads(rendered_payload_str)

                    content_type = template_config.get("content_type", "json").lower()
                    if content_type == "json":
                        request_kwargs["json"] = payload
                    elif content_type == "form":
                        request_kwargs["data"] = payload
                else:
                    # It's valid for a POST to have no body, so we just log a debug message.
                    log.debug(
                        "No payload_template found for method, sending request with empty body.",
                        method=http_method,
                    )

            # Use the flexible `client.request` to handle all HTTP methods.
            response = await client.request(http_method, api_endpoint, **request_kwargs)

            response.raise_for_status()

            # Gracefully handle responses that have no content (e.g., HTTP 204 No Content).
            return (
                response.json()
                if response.content
                else {"status_code": response.status_code, "content": None}
            )
