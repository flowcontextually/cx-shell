from typing import List, Dict, Any

from ..engine.connector.config import CX_HOME
from ..engine.connector.service import ConnectorService
from ..interactive.session import SessionState
from ..interactive.commands import create_script_for_step
from cx_core_schemas.connector_script import ConnectorStep, RunSqlQueryAction

QUERIES_DIR = CX_HOME / "queries"


class QueryManager:
    """Handles listing and running .sql files."""

    def __init__(self):
        QUERIES_DIR.mkdir(exist_ok=True, parents=True)

    def list_queries(self) -> List[Dict[str, str]]:
        """Lists all available queries, returning structured data."""
        queries_data = []
        query_files = list(QUERIES_DIR.glob("*.sql"))
        if not query_files:
            return queries_data

        for q_file in sorted(query_files):
            queries_data.append({"Name": q_file.stem})

        return queries_data

    async def run_query(
        self,
        state: SessionState,
        service: ConnectorService,
        name: str,
        on_alias: str,
        args: Dict[str, Any],
    ) -> Any:
        query_file = QUERIES_DIR / f"{name}.sql"
        if not query_file.exists():
            raise FileNotFoundError(f"Query '{name}' not found at {query_file}")
        if on_alias not in state.connections:
            raise ValueError(f"Connection alias '{on_alias}' is not active.")

        connection_source = state.connections[on_alias]
        query_content = query_file.read_text()
        step = ConnectorStep(
            id=f"interactive_query_{name}",
            name=f"Interactive query {name}",
            connection_source=connection_source,
            run=RunSqlQueryAction(
                action="run_sql_query", query=query_content, parameters=args
            ),
        )
        script = create_script_for_step(step)

        from rich.console import Console

        Console().print(
            f"[bold blue]Running query '{name}' on connection '{on_alias}'...[/bold blue]"
        )

        results = await service.engine.run_script_model(
            script, session_variables=state.variables
        )
        return results.get(step.name)
