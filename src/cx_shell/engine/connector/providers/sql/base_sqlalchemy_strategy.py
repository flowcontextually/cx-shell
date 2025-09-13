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
            engine = create_async_engine(connection_url)
            log.info("sqlalchemy.engine.created")
            yield engine
        finally:
            if engine:
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
        Executes a SQL query, manually handling parameter expansion for IN clauses
        to ensure compatibility with drivers like pyodbc.
        """
        log = logger.bind(connection_id=connection.id, dialect=self.dialect_driver)
        log.info("sqlalchemy.execute_query.begin")

        connection_url = self._get_connection_url(connection, secrets)
        engine = create_async_engine(connection_url)

        try:
            final_query = query
            final_params = params.copy()
            list_params = {k: v for k, v in final_params.items() if isinstance(v, list)}

            if list_params:
                log.info(
                    "Manually expanding list parameters for IN clause.",
                    params=list(list_params.keys()),
                )
                for key, values in list_params.items():
                    if not values:  # Handle empty lists to avoid invalid SQL
                        final_query = final_query.replace(f"(:{key})", "(NULL)")
                        del final_params[key]
                        continue
                    new_param_names = [f"{key}_{i}" for i in range(len(values))]
                    placeholders = ", ".join([f":{p}" for p in new_param_names])
                    final_query = final_query.replace(f"(:{key})", f"({placeholders})")
                    del final_params[key]
                    final_params.update(zip(new_param_names, values))

            stmt = text(final_query)
            log.info("sqlalchemy.execute_query.executing", final_params=final_params)

            async with engine.connect() as conn:
                result_proxy = await conn.execute(stmt, final_params)

                # --- DEFINITIVE FIX for "no rows" queries ---
                # Before trying to fetch results, check if the query was expected to return rows.
                if result_proxy.returns_rows:
                    mapping_results = result_proxy.mappings().all()
                    log.info(
                        "sqlalchemy.execute_query.success",
                        row_count=len(mapping_results),
                    )
                    dict_results = [dict(row) for row in mapping_results]
                    return safe_serialize(dict_results)
                else:
                    # If no rows were returned (e.g., a comment, DDL, or an UPDATE statement),
                    # return an empty list, which is the correct representation of "no data".
                    log.info(
                        "sqlalchemy.execute_query.success_no_rows",
                        row_count=result_proxy.rowcount,
                    )
                    return []
                # --- END FIX ---

        except Exception as e:
            log.error("sqlalchemy.execute_query.failed", error=str(e), exc_info=True)
            raise IOError(f"Query execution failed: {e}") from e
        finally:
            await engine.dispose()

    async def browse_path(
        self, path_parts: List[str], connection: "Connection", secrets: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        logger.warning("browse_path.not_implemented", strategy_key=self.strategy_key)
        return []

    async def get_content(
        self, path_parts: List[str], connection: "Connection", secrets: Dict[str, Any]
    ) -> "VfsFileContentResponse":
        logger.warning("get_content.not_implemented", strategy_key=self.strategy_key)
        raise NotImplementedError(
            "get_content is not implemented for this SQL strategy."
        )
