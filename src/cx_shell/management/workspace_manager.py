# [CREATE NEW FILE] /home/dpwanjala/repositories/cx-shell/src/cx_shell/management/workspace_manager.py

import json
from pathlib import Path
from typing import List, Dict

import structlog
from rich.console import Console
from rich.table import Table

from ..utils import CX_HOME

logger = structlog.get_logger(__name__)
WORKSPACE_FILE = CX_HOME / "workspace.json"
console = Console()


class WorkspaceManager:
    """Manages the user's project roots via the workspace.json manifest."""

    def _load_manifest(self) -> Dict:
        """Loads the workspace manifest file."""
        if not WORKSPACE_FILE.exists():
            return {"roots": []}
        try:
            return json.loads(WORKSPACE_FILE.read_text())
        except (json.JSONDecodeError, FileNotFoundError):
            return {"roots": []}

    def _save_manifest(self, manifest_data: Dict):
        """Saves the workspace manifest file."""
        WORKSPACE_FILE.write_text(json.dumps(manifest_data, indent=2))

    def get_roots(self) -> List[Path]:
        """
        Gets a list of all active project root paths.
        The system root (~/.cx) is always implicitly included.
        """
        manifest = self._load_manifest()
        # Always include the system root for installed apps
        roots = [CX_HOME]
        for path_str in manifest.get("roots", []):
            roots.append(Path(path_str).expanduser().resolve())
        return roots

    def list_roots(self):
        """Displays a table of registered project roots."""
        manifest = self._load_manifest()
        roots = manifest.get("roots", [])

        table = Table(title="Registered Workspace Project Roots")
        table.add_column("Path", style="cyan")
        table.add_column("Status", style="green")

        if not roots:
            console.print(
                "[dim]No user-defined project roots. Use `cx workspace add` to register a project.[/dim]"
            )
        else:
            for path_str in roots:
                path = Path(path_str).expanduser()
                status = "✅ Found" if path.is_dir() else "[red]✗ Not Found[/red]"
                table.add_row(str(path), status)
            console.print(table)

        console.print(f"\n[dim]System root (always included): {CX_HOME}[/dim]")

    def add_root(self, path_str: str):
        """Adds a new project root to the workspace."""
        path_to_add = Path(path_str).expanduser().resolve()
        if not path_to_add.is_dir():
            console.print(
                f"[bold red]Error:[/bold red] Path '{path_to_add}' is not a valid directory."
            )
            return

        manifest = self._load_manifest()
        # Use strings for JSON serialization and to handle ~ correctly
        root_str_to_add = (
            f"~/{path_to_add.relative_to(Path.home())}"
            if path_to_add.is_relative_to(Path.home())
            else str(path_to_add)
        )

        if root_str_to_add not in manifest["roots"]:
            manifest["roots"].append(root_str_to_add)
            manifest["roots"].sort()
            self._save_manifest(manifest)
            console.print(f"✅ Added '[cyan]{path_to_add}[/cyan]' to your workspace.")
        else:
            console.print(
                f"[yellow]Path '[cyan]{path_to_add}[/cyan]' is already in your workspace.[/yellow]"
            )

    def remove_root(self, path_str: str):
        """Removes a project root from the workspace."""
        path_to_remove = Path(path_str).expanduser().resolve()
        manifest = self._load_manifest()

        # Find the matching string representation to remove
        root_str_to_remove = None
        for root in manifest["roots"]:
            if Path(root).expanduser().resolve() == path_to_remove:
                root_str_to_remove = root
                break

        if root_str_to_remove:
            manifest["roots"].remove(root_str_to_remove)
            self._save_manifest(manifest)
            console.print(
                f"✅ Removed '[cyan]{path_to_remove}[/cyan]' from your workspace."
            )
        else:
            console.print(
                f"[bold red]Error:[/bold red] Path '{path_to_remove}' not found in your workspace roots."
            )
