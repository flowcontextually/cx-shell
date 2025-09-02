from contextlib import asynccontextmanager
from typing import Any, Dict, List, TYPE_CHECKING

import structlog
from sqlalchemy import text, bindparam
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine

from ...utils import safe_serialize
from ..base import BaseConnectorStrategy
from .....state import APP_STATE


if TYPE_CHECKING:
    from cx_core_schemas.connection import Connection
    from cx_core_schemas.vfs import VfsFileContentResponse


logger = structlog.get_logger(__name__)


class BaseSqlAlchemyStrategy(BaseConnectorStrategy):
    """
    A reusable, production-grade base strategy for connecting to any
    SQLAlchemy-compatible database using its asyncio interface.
    """

    dialect_driver: str = ""

    def _get_connection_url(
        self, connection: "Connection", secrets: Dict[str, Any]
    ) -> str:
        """Constructs the SQLAlchemy connection URL. Must be implemented by subclass."""
        raise NotImplementedError

    @asynccontextmanager
    async def get_client(self, connection: "Connection", secrets: Dict[str, Any]):
        """
        Provides a ready-to-use, pooled SQLAlchemy async engine.
        The engine is the primary client for all operations.
        """
        engine: AsyncEngine | None = None
        log = logger.bind(connection_id=connection.id, dialect=self.dialect_driver)
        try:
            connection_url = self._get_connection_url(connection, secrets)
            # Create a new engine with connection pooling enabled by default.
            engine = create_async_engine(connection_url)
            log.info("sqlalchemy.engine.created")
            yield engine
        finally:
            if engine:
                # Dispose cleanly closes all connections in the pool.
                await engine.dispose()
                log.info("sqlalchemy.engine.disposed")

    async def test_connection(
        self, connection: "Connection", secrets: Dict[str, Any]
    ) -> bool:
        """Tests the connection by executing a simple 'SELECT 1' query."""
        log = logger.bind(connection_id=connection.id, dialect=self.dialect_driver)
        log.info("sqlalchemy.test_connection.begin")
        try:
            async with self.get_client(connection, secrets) as engine:
                async with engine.connect() as conn:
                    result = await conn.execute(text("SELECT 1"))
                    if result.scalar_one() != 1:
                        raise ConnectionError("Test query 'SELECT 1' did not return 1.")
            log.info("sqlalchemy.test_connection.success")
            return True
        except Exception as e:
            # Log the error, but only include the full traceback if in verbose mode.
            log.error(
                "sqlalchemy.test_connection.failed",
                error=str(e),
                exc_info=APP_STATE.verbose_mode,
            )
            raise ConnectionError(f"Database connection test failed: {e}") from e

    async def execute_query(
        self,
        query: str,
        params: Dict,
        connection: "Connection",
        secrets: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """
        Executes a SQL query using SQLAlchemy's async engine, with robust
        handling for list parameters in IN clauses, and returns the results
        as a list of standard Python dictionaries.
        """
        log = logger.bind(connection_id=connection.id, dialect=self.dialect_driver)
        log.info("sqlalchemy.execute_query.begin")

        connection_url = self._get_connection_url(connection, secrets)
        engine = create_async_engine(connection_url)

        try:
            # Convert the query to a SQLAlchemy `text()` object for structured parameter binding.
            stmt = text(query)

            # Check for any parameters whose value is a list for IN clause expansion.
            list_params = {
                key for key, value in params.items() if isinstance(value, list)
            }

            if list_params:
                # For each list parameter, re-bind it with the `expanding=True` flag.
                # This tells SQLAlchemy to automatically generate the correct placeholders.
                for key in list_params:
                    stmt = stmt.bindparams(bindparam(key, expanding=True))

            log.info(
                "sqlalchemy.execute_query.prepared_statement",
                params=params,
                expanding_params=list(list_params),
            )

            async with engine.connect() as conn:
                # Execute the statement with the original parameters dictionary.
                # SQLAlchemy handles the expansion and dialect-specific placeholder generation.
                result_proxy = await conn.execute(stmt, params)
                # .mappings().all() returns a list of dictionary-like RowMapping objects.
                mapping_results = result_proxy.mappings().all()

            log.info("sqlalchemy.execute_query.success", row_count=len(mapping_results))

            # --- DEFINITIVE FIX for JSON Serialization ---
            # Explicitly convert each RowMapping object into a plain Python dictionary.
            dict_results = [dict(row) for row in mapping_results]

            # Now, run the clean list of dictionaries through our serializer
            # to handle special data types like datetimes and decimals.
            return safe_serialize(dict_results)
            # --- END FIX ---

        except Exception as e:
            log.error("sqlalchemy.execute_query.failed", error=str(e), exc_info=True)
            # Re-raise as a standard IOError for the engine to catch gracefully.
            raise IOError(f"Query execution failed: {e}") from e
        finally:
            # Cleanly close all connections in the engine's pool.
            await engine.dispose()

    # --- Fulfilling the Rest of the Contract (with placeholder implementations) ---

    async def browse_path(
        self, path_parts: List[str], connection: "Connection", secrets: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Browsing database schemas, tables, etc. is highly dialect-specific."""
        logger.warning("browse_path.not_implemented", strategy_key=self.strategy_key)
        # A full implementation would use SQLAlchemy's Inspector to get metadata.
        # e.g., from sqlalchemy import inspect
        #       inspector = inspect(engine)
        #       schemas = await inspector.get_schema_names()
        return []

    async def get_content(
        self, path_parts: List[str], connection: "Connection", secrets: Dict[str, Any]
    ) -> "VfsFileContentResponse":
        """Getting content for a specific table would typically involve running a SELECT query."""
        logger.warning("get_content.not_implemented", strategy_key=self.strategy_key)
        raise NotImplementedError(
            "get_content is not implemented for this SQL strategy."
        )
