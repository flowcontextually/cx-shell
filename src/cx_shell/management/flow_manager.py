import yaml
from typing import List, Dict, Any

from ..engine.connector.config import CX_HOME
from ..engine.connector.service import ConnectorService
from ..interactive.session import SessionState

FLOWS_DIR = CX_HOME / "flows"


class FlowManager:
    """Handles logic for listing and running .flow.yaml files."""

    def __init__(self):
        FLOWS_DIR.mkdir(exist_ok=True, parents=True)

    def list_flows(self) -> List[Dict[str, str]]:
        """Lists all available flows, returning structured data."""
        flows_data = []
        flow_files = list(FLOWS_DIR.glob("*.flow.yaml"))
        if not flow_files:
            return flows_data

        for flow_file in sorted(flow_files):
            try:
                with open(flow_file, "r") as f:
                    data = yaml.safe_load(f)
                    description = data.get("description", "No description.")
                flows_data.append(
                    {
                        "Name": flow_file.stem.replace(".flow", ""),
                        "Description": description,
                    }
                )
            except Exception:
                flows_data.append(
                    {
                        "Name": f"[red]{flow_file.stem}[/red]",
                        "Description": "[red]Error reading file[/red]",
                    }
                )
        return flows_data

    async def run_flow(
        self,
        state: SessionState,
        service: ConnectorService,
        name: str,
        args: Dict[str, Any],
    ) -> Any:
        flow_file = FLOWS_DIR / f"{name}.flow.yaml"
        if not flow_file.exists():
            raise FileNotFoundError(f"Flow '{name}' not found at {flow_file}")

        # We need the console here for user feedback in the REPL
        from rich.console import Console

        Console().print(f"[bold blue]Running flow '{name}'...[/bold blue]")

        return await service.engine.run_script(
            flow_file, script_input=args, session_variables=state.variables
        )
