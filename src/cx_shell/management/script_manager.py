import json
from typing import Dict, Any

from rich.console import Console
from rich.table import Table
from rich import box

from ..engine.connector.config import CX_HOME
from ..engine.connector.service import ConnectorService
from ..interactive.session import SessionState
from ..engine.connector.utils import safe_serialize
from cx_core_schemas.connector_script import (
    ConnectorScript,
    ConnectorStep,
    RunPythonScriptAction,
)

SCRIPTS_DIR = CX_HOME / "scripts"
console = Console()


class ScriptManager:
    """Handles logic for listing and running .py files."""

    def __init__(self):
        SCRIPTS_DIR.mkdir(exist_ok=True, parents=True)

    def list_scripts(self):
        scripts = list(SCRIPTS_DIR.glob("*.py"))
        if not scripts:
            console.print("No scripts found in ~/.cx/scripts/")
            return

        table = Table(title="Available Scripts", box=box.ROUNDED)
        table.add_column("Script Name", style="cyan")
        for script_file in sorted(scripts):
            table.add_row(script_file.stem)
        console.print(table)

    async def run_script(
        self,
        state: SessionState,
        service: ConnectorService,
        name: str,
        args: Dict[str, Any],
        piped_input: Any,
    ) -> Any:
        script_file = SCRIPTS_DIR / f"{name}.py"
        if not script_file.exists():
            raise FileNotFoundError(f"Script '{name}' not found at {script_file}")

        input_data = {**(piped_input or {}), **args}
        serializable_input = safe_serialize(input_data)

        step = ConnectorStep(
            id=f"run_script_{name}",
            name=f"Run Python Script: {name}",
            connection_source="user:system_python_sandbox",
            run=RunPythonScriptAction(
                action="run_python_script",
                script_path=str(script_file),
                input_data_json=json.dumps(serializable_input),
            ),
        )
        script = ConnectorScript(
            name=f"Interactive Script run for {name}.py", steps=[step]
        )

        console.print(f"[bold blue]Running script '{name}'...[/bold blue]")
        results = await service.engine.run_script_model(
            script, session_variables=state.variables
        )
        return results.get(step.name)
