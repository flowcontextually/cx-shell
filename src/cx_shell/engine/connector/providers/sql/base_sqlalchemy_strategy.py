from contextlib import asynccontextmanager
from typing import Any, Dict, List, TYPE_CHECKING

import structlog
from sqlalchemy import text
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

    # async def execute_query(
    #     self,
    #     query: str,
    #     params: Dict,
    #     connection: "Connection",
    #     secrets: Dict[str, Any],
    # ) -> List[Dict[str, Any]]:
    #     """
    #     Executes a SQL query using SQLAlchemy's async engine, with robust
    #     handling for list parameters in IN clauses, and returns the results
    #     as a list of standard Python dictionaries.
    #     """
    #     log = logger.bind(connection_id=connection.id, dialect=self.dialect_driver)
    #     log.info("sqlalchemy.execute_query.begin")

    #     connection_url = self._get_connection_url(connection, secrets)
    #     engine = create_async_engine(connection_url)

    #     try:
    #         # Convert the query to a SQLAlchemy `text()` object for structured parameter binding.
    #         stmt = text(query)

    #         # --- STRATEGIC DIAGNOSTIC LOG ---
    #         # Log the received parameters and their types BEFORE any processing.
    #         param_types = {k: type(v).__name__ for k, v in params.items()}
    #         log.critical(
    #             "sqlalchemy.execute_query.received_params",
    #             params=params,
    #             param_types=param_types,
    #         )
    #         # --- END DIAGNOSTIC LOG ---

    #         # Check for any parameters whose value is a list for IN clause expansion.
    #         list_params = {
    #             key for key, value in params.items() if isinstance(value, list)
    #         }

    #         if list_params:
    #             # For each list parameter, re-bind it with the `expanding=True` flag.
    #             # This tells SQLAlchemy to automatically generate the correct placeholders.
    #             for key in list_params:
    #                 stmt = stmt.bindparams(bindparam(key, expanding=True))

    #         log.info(
    #             "sqlalchemy.execute_query.prepared_statement",
    #             params=params,
    #             expanding_params=list(list_params),
    #         )

    #         async with engine.connect() as conn:
    #             # Execute the statement with the original parameters dictionary.
    #             # SQLAlchemy handles the expansion and dialect-specific placeholder generation.
    #             result_proxy = await conn.execute(stmt, params)
    #             # .mappings().all() returns a list of dictionary-like RowMapping objects.
    #             mapping_results = result_proxy.mappings().all()

    #         log.info("sqlalchemy.execute_query.success", row_count=len(mapping_results))

    #         # --- DEFINITIVE FIX for JSON Serialization ---
    #         # Explicitly convert each RowMapping object into a plain Python dictionary.
    #         dict_results = [dict(row) for row in mapping_results]

    #         # Now, run the clean list of dictionaries through our serializer
    #         # to handle special data types like datetimes and decimals.
    #         return safe_serialize(dict_results)
    #         # --- END FIX ---

    #     except Exception as e:
    #         log.error("sqlalchemy.execute_query.failed", error=str(e), exc_info=True)
    #         # Re-raise as a standard IOError for the engine to catch gracefully.
    #         raise IOError(f"Query execution failed: {e}") from e
    #     finally:
    #         # Cleanly close all connections in the engine's pool.
    #         await engine.dispose()

    async def execute_query(
        self,
        query: str,
        params: Dict,
        connection: "Connection",
        secrets: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """
        Executes a SQL query, manually handling parameter expansion for IN clauses
        to ensure compatibility with drivers like pyodbc.
        """
        log = logger.bind(connection_id=connection.id, dialect=self.dialect_driver)
        log.info("sqlalchemy.execute_query.begin")

        connection_url = self._get_connection_url(connection, secrets)
        engine = create_async_engine(connection_url)

        try:
            # --- THIS IS THE FINAL FIX ---
            final_query = query
            final_params = params.copy()

            list_params = {k: v for k, v in final_params.items() if isinstance(v, list)}

            if list_params:
                log.info(
                    "Manually expanding list parameters for IN clause.",
                    params=list(list_params.keys()),
                )
                for key, values in list_params.items():
                    # Create a list of new, unique parameter names, e.g., ['agency_list_0', 'agency_list_1']
                    new_param_names = [f"{key}_{i}" for i in range(len(values))]

                    # Create the placeholder string, e.g., ":agency_list_0, :agency_list_1"
                    placeholders = ", ".join([f":{p}" for p in new_param_names])

                    # Replace the single placeholder in the query with the new list of placeholders
                    final_query = final_query.replace(f"(:{key})", f"({placeholders})")

                    # Remove the old list parameter from the dictionary
                    del final_params[key]

                    # Add the new, individual parameters to the dictionary
                    final_params.update(zip(new_param_names, values))

            stmt = text(final_query)
            # --- END FIX ---

            log.info("sqlalchemy.execute_query.executing", final_params=final_params)

            async with engine.connect() as conn:
                result_proxy = await conn.execute(stmt, final_params)
                mapping_results = result_proxy.mappings().all()

            log.info("sqlalchemy.execute_query.success", row_count=len(mapping_results))
            dict_results = [dict(row) for row in mapping_results]
            return safe_serialize(dict_results)

        except Exception as e:
            log.error("sqlalchemy.execute_query.failed", error=str(e), exc_info=True)
            raise IOError(f"Query execution failed: {e}") from e
        finally:
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
