import yaml
from typing import Dict, Any

from rich.console import Console
from rich.table import Table
from rich import box

from ..engine.connector.config import CX_HOME
from ..engine.connector.service import ConnectorService
from ..interactive.session import SessionState

FLOWS_DIR = CX_HOME / "flows"
console = Console()


class FlowManager:
    """Handles logic for listing and running .flow.yaml files."""

    def __init__(self):
        FLOWS_DIR.mkdir(exist_ok=True, parents=True)

    def list_flows(self):
        flows = list(FLOWS_DIR.glob("*.flow.yaml"))
        if not flows:
            console.print("No flows found in ~/.cx/flows/")
            return

        table = Table(title="Available Flows", box=box.ROUNDED)
        table.add_column("Flow Name", style="cyan")
        table.add_column("Description", style="dim", overflow="fold")

        for flow_file in sorted(flows):
            try:
                with open(flow_file, "r") as f:
                    data = yaml.safe_load(f)
                    description = data.get("description", "No description.")
                # Use flow_file.stem to get the filename without the extension.
                table.add_row(flow_file.stem.replace(".flow", ""), description)
            except Exception:
                table.add_row(
                    f"[red]{flow_file.stem}[/red]", "[red]Error reading file[/red]"
                )

        console.print(table)

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

        with open(flow_file, "r") as f:
            script_data = yaml.safe_load(f)

        # The 'args' dict from the command line becomes the script_input
        console.print(f"[bold blue]Running flow '{name}'...[/bold blue]")
        return await service.engine.run_script(
            flow_file,
            script_input=args,
            # We don't pass session_variables here by default, to keep flows self-contained.
            # This is a design choice we can revisit if needed.
        )
