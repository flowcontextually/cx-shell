from typing import Dict, Any

from rich.console import Console
from rich.table import Table
from rich import box

from ..engine.connector.config import CX_HOME
from ..engine.connector.service import ConnectorService
from ..interactive.session import SessionState
from ..interactive.commands import create_script_for_step
from cx_core_schemas.connector_script import ConnectorStep, RunSqlQueryAction

QUERIES_DIR = CX_HOME / "queries"
console = Console()


class QueryManager:
    """Handles listing and running .sql files."""

    def __init__(self):
        QUERIES_DIR.mkdir(exist_ok=True, parents=True)

    def list_queries(self):
        queries = list(QUERIES_DIR.glob("*.sql"))
        if not queries:
            console.print("No queries found in ~/.cx/queries/")
            return

        table = Table(title="Available Queries", box=box.ROUNDED)
        table.add_column("Query Name", style="cyan")
        for q_file in sorted(queries):
            table.add_row(q_file.stem)
        console.print(table)

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

        # --- THIS IS THE FIX ---
        # Look up the alias in the current session state to get the true connection source.
        if on_alias not in state.connections:
            raise ValueError(
                f"Connection alias '{on_alias}' is not active in the current session. Use 'connections' to see active aliases."
            )

        connection_source = state.connections[on_alias]
        # --- END FIX ---

        query_content = query_file.read_text()

        step = ConnectorStep(
            id=f"interactive_query_{name}",
            name=f"Interactive query {name}",
            connection_source=connection_source,  # Use the resolved source
            run=RunSqlQueryAction(
                action="run_sql_query", query=query_content, parameters=args
            ),
        )
        script = create_script_for_step(step)

        console.print(
            f"[bold blue]Running query '{name}' on connection '{on_alias}' ({connection_source})...[/bold blue]"
        )
        results = await service.engine.run_script_model(
            script, session_variables=state.variables
        )
        return results.get(step.name)
