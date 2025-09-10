# [REPLACE] /home/dpwanjala/repositories/connector-logic/src/connector_logic/service.py

from contextlib import asynccontextmanager
import os
from pathlib import Path
from typing import Any, Dict, Optional, TYPE_CHECKING

import hvac
import structlog
from cx_core_schemas.connection import Connection
from surrealdb import AsyncSurreal

# --- Local & Shared Imports ---
from .config import ConnectionResolver
from .providers.base import BaseConnectorStrategy
from .providers.git.declarative_git_strategy import DeclarativeGitStrategy
from .providers.oauth.declarative_oauth_strategy import DeclarativeOauthStrategy
from .providers.rest.api_key_strategy import ApiKeyStrategy
from .providers.rest.declarative_strategy import DeclarativeRestStrategy
from .providers.rest.webhook_strategy import WebhookStrategy
from .providers.sql.mssql_strategy import MssqlStrategy
from .providers.sql.trino_strategy import TrinoStrategy
from .providers.fs.declarative_fs_strategy import DeclarativeFilesystemStrategy
from .providers.py.sandboxed_python_strategy import SandboxedPythonStrategy
from .providers.internal.smart_fetcher_strategy import SmartFetcherStrategy
from .providers.browser.strategy import DeclarativeBrowserStrategy
from ...state import APP_STATE


from .vfs_reader import LocalVfsReader

if TYPE_CHECKING:
    from .engine import ScriptEngine

logger = structlog.get_logger(__name__)


class ConnectorService:
    """
    The main Connector Service. Manages I/O strategies and provides
    programmatic and script-based access to external services. It can operate
    in a fully integrated mode (with DB/Vault) or a standalone mode (with local files).
    """

    def __init__(
        self,
        db_client: Optional[AsyncSurreal] = None,
        vault_client: Optional[hvac.Client] = None,
        cx_home_path: Optional[Path] = None,
    ):
        """
        Initializes the service with its dependencies. If clients are not provided,
        it operates in a standalone mode, relying on local configuration files.

        Args:
            db_client: An optional asynchronous SurrealDB client instance.
            vault_client: An optional hvac (HashiCorp Vault) client instance.
        """
        self.db = db_client
        self.vault = vault_client
        self.git_cache_root = os.getenv(
            "GIT_CACHE_ROOT", "/tmp/cgi_git_cache_service_default"
        )
        self.vault_mount_point = os.getenv("VAULT_SECRET_MOUNT_POINT", "secret")
        self.strategies: Dict[str, BaseConnectorStrategy] = {}
        self._register_strategies()

        # Late import to prevent circular dependency issues
        from .engine import ScriptEngine

        # The service now owns the resolver and engine, configured for the correct mode.
        self.resolver = ConnectionResolver(
            db_client, vault_client, cx_home_path=cx_home_path
        )
        self.engine: "ScriptEngine" = ScriptEngine(self.resolver, self)

        logger.info(
            "ConnectorService initialized.",
            strategy_count=len(self.strategies),
            mode="standalone" if self.resolver.is_standalone else "integrated",
        )

    def _register_strategies(self):
        """
        Discovers and registers all concrete strategy classes, correctly
        handling dependency injection for meta-strategies.
        """
        vfs_reader = LocalVfsReader()

        # --- Stage 1: Instantiate and Register Base Protocol Engines ---

        base_strategy_classes = [
            DeclarativeRestStrategy,
            ApiKeyStrategy,
            WebhookStrategy,
            MssqlStrategy,
            DeclarativeOauthStrategy,
            DeclarativeGitStrategy,
            TrinoStrategy,
            DeclarativeFilesystemStrategy,
            SandboxedPythonStrategy,
            DeclarativeBrowserStrategy,
        ]

        # A temporary dictionary to hold the instances we create
        strategy_instances = {}

        for strategy_cls in base_strategy_classes:
            if strategy_cls.strategy_key:
                strategy_kwargs = {
                    "vfs_reader": vfs_reader,
                    "vault_client": self.vault,
                }
                if "git" in strategy_cls.strategy_key:
                    strategy_kwargs["git_cache_root"] = self.git_cache_root
                if "oauth" in strategy_cls.strategy_key:
                    strategy_kwargs["vault_mount_point"] = self.vault_mount_point

                # Create the instance
                instance = strategy_cls(**strategy_kwargs)

                # Store the instance and add it to the main strategies registry
                strategy_instances[strategy_cls.strategy_key] = instance
                self.strategies[instance.strategy_key] = instance

                logger.debug(
                    "Registered base strategy.", strategy_key=instance.strategy_key
                )

        # --- Stage 2: Instantiate and Register Meta Strategies ---

        # Now that we have instances of the base strategies, we can create
        # any meta-strategies that depend on them.

        smart_fetcher = SmartFetcherStrategy(
            # Dependency Injection: Pass the required strategy instances.
            fs_strategy=strategy_instances["fs-declarative"],
            rest_strategy=strategy_instances["rest-declarative"],
        )
        self.strategies[smart_fetcher.strategy_key] = smart_fetcher
        logger.debug(
            "Registered meta strategy.", strategy_key=smart_fetcher.strategy_key
        )

    def _get_strategy_for_connection_model(
        self, connection: Connection
    ) -> BaseConnectorStrategy:
        """
        Finds the correct strategy instance based on the connection's
        embedded ApiCatalog record.

        Args:
            connection: The Pydantic model of the connection.

        Returns:
            The corresponding strategy instance.

        Raises:
            ValueError: If the connection is missing catalog data or the provider key.
            NotImplementedError: If no strategy is registered for the given key.
        """
        log = logger.bind(connection_name=connection.name, connection_id=connection.id)

        if not connection.catalog:
            log.error("connection_missing_catalog_data")
            raise ValueError(
                f"Connection '{connection.name}' is missing embedded catalog data."
            )

        strategy_key = connection.catalog.connector_provider_key
        if not strategy_key:
            log.error("catalog_missing_provider_key", catalog_id=connection.catalog.id)
            raise ValueError(
                f"ApiCatalog for '{connection.name}' is missing 'connector_provider_key'."
            )

        strategy = self.strategies.get(strategy_key)
        if not strategy:
            log.error("strategy_not_registered", strategy_key=strategy_key)
            raise NotImplementedError(
                f"No connector strategy registered for key '{strategy_key}'."
            )
        return strategy

    async def run_script(
        self,
        script_path: Path,
        script_input: Dict[str, Any] = {},
        debug_mode: bool = False,
    ) -> Dict[str, Any]:
        """
        Delegates script execution to the script engine, passing all context.

        This method acts as the primary entry point from the CLI and other services.

        Args:
            script_path: The path to the script file.
            script_input: A dictionary of data piped from a previous command's stdout.
            debug_mode: If True, enables debug features in the engine.

        Returns:
            A dictionary containing the results of the script execution.
        """
        return await self.engine.run_script(script_path, script_input, debug_mode)

    async def test_connection(self, connection_source: str) -> Dict[str, Any]:
        """
        Tests a connection from any valid source.

        This method encapsulates the entire connection testing logic. It resolves
        the connection and its secrets, finds the correct strategy, and executes
        the test. It is designed to never raise an exception, instead returning
        a dictionary with a 'status' and 'message' key.

        Args:
            connection_source: The identifier for the connection (e.g., 'user:my-db').

        Returns:
            A dictionary indicating the outcome, e.g.,
            {'status': 'success', 'message': '...'} or
            {'status': 'error', 'message': 'Login failed...'}.
        """
        log = logger.bind(connection_source=connection_source)
        connection_id_for_update = None
        connection_name = "unknown"

        try:
            if connection_source.startswith("db:"):
                connection_id_for_update = connection_source.split(":", 1)[1]

            connection, secrets = await self.resolver.resolve(connection_source)
            connection_name = connection.name
            strategy = self._get_strategy_for_connection_model(connection)

            # Delegate the actual test to the appropriate strategy.
            await strategy.test_connection(connection, secrets)

            # --- Success Path ---
            # In a future integrated mode, this is where we would update the DB status.
            if connection_id_for_update and self.db:
                # Placeholder for DB update logic
                pass

            log.info("Connection test successful.", connection_name=connection_name)
            return {
                "status": "success",
                "message": f"Connection test for '{connection_name}' successful.",
            }

        except Exception as e:
            # --- Failure Path ---
            # Catch ANY exception from the process (e.g., file not found, login failed, network error)
            # and package it into a clean, user-facing error message.
            # Only include the verbose traceback if the user asked for it.
            log.error(
                "Connection test failed.",
                connection_name=connection_name,
                error=str(e),
                exc_info=APP_STATE.verbose_mode,
            )
            # In a future integrated mode, update the DB status to 'error'.
            if connection_id_for_update and self.db:
                # Placeholder for DB update logic
                pass

            return {"status": "error", "message": str(e)}

    @asynccontextmanager
    async def get_client(self, connection_source: str):
        """
        Provides a ready-to-use, authenticated client for a given service.

        This is an async context manager that ensures proper cleanup of resources.

        Args:
            connection_source: The source identifier (e.g., 'db:conn:123', 'file:./path').

        Yields:
            An authenticated client object (e.g., httpx.AsyncClient, SQLAlchemy Engine).

        Raises:
            Exception: Propagates any error from the connection resolution or
                       client initialization process.
        """
        log = logger.bind(connection_source=connection_source)
        try:
            connection, secrets = await self.resolver.resolve(connection_source)
            strategy = self._get_strategy_for_connection_model(connection)

            # Yield the client from the strategy's context manager
            async with strategy.get_client(connection, secrets) as client:
                yield client
        except Exception as e:
            log.error(
                "Failed to get client for connection.", error=str(e), exc_info=True
            )
            # Re-raise the exception to be handled by the caller
            raise
