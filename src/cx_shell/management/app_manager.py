# src/cx_shell/management/app_manager.py
import asyncio
import json
import shutil
import tarfile
import tempfile
from pathlib import Path
from typing import Dict, Any, List

import httpx
import yaml
from rich.console import Console
from rich.table import Table
from prompt_toolkit import PromptSession

from ..engine.connector.config import ConnectionResolver, CX_HOME
from .registry_manager import RegistryManager

console = Console()

APPS_REGISTRY_URL = (
    "https://raw.githubusercontent.com/flowcontextually/applications/main/registry.yaml"
)
APPS_DOWNLOAD_URL_TEMPLATE = "https://github.com/flowcontextually/applications/releases/download/{tag}/{asset_name}"
APPS_MANIFEST_FILE = CX_HOME / "apps.json"


class AppManager:
    """A service for discovering, installing, and managing Contextually Applications."""

    def __init__(self):
        self.resolver = ConnectionResolver()
        self.registry_manager = RegistryManager()
        CX_HOME.mkdir(exist_ok=True, parents=True)

    # --- NEW HELPER: Load/Save the local apps manifest ---
    def _load_local_manifest(self) -> Dict[str, Any]:
        if not APPS_MANIFEST_FILE.exists():
            return {"installed_apps": {}}
        with open(APPS_MANIFEST_FILE, "r") as f:
            return json.load(f)

    def _save_local_manifest(self, manifest_data: Dict[str, Any]):
        with open(APPS_MANIFEST_FILE, "w") as f:
            json.dump(manifest_data, f, indent=2)

    async def search(self, query: str | None = None):
        # ... (This method is complete and correct, no changes needed)
        with console.status("Fetching public application registry..."):
            try:
                apps = await self.registry_manager.get_available_applications()
            except Exception as e:
                console.print(
                    f"[bold red]Error:[/bold red] Could not fetch or parse the application registry. {e}"
                )
                return

        table = Table(title="Publicly Available Applications")
        table.add_column("ID", style="cyan", no_wrap=True)
        table.add_column("Version", style="magenta")
        table.add_column("Description", overflow="fold")

        for app in apps:
            display = True
            if query:
                search_text = f"{app.get('id', '')} {app.get('description', '')} {' '.join(app.get('tags', []))}".lower()
                if query.lower() not in search_text:
                    display = False
            if display:
                table.add_row(app.get("id"), app.get("version"), app.get("description"))

        console.print(table)

    async def install(self, app_id: str, no_interactive: bool = False):
        """
        Installs or updates an application from the public registry or a URL.
        Includes asset installation, manifest tracking, and an interactive
        connection setup wizard.
        """
        # TODO: Add logic to handle installing from a direct URL in addition to app_id

        local_manifest = self._load_local_manifest()
        if app_id in local_manifest["installed_apps"]:
            console.print(
                f"[yellow]Application '{app_id}' is already installed. To update, please uninstall and reinstall.[/yellow]"
            )
            return

        with console.status("Resolving application from registry..."):
            apps = await self.registry_manager.get_available_applications()
            app_meta = next((app for app in apps if app.get("id") == app_id), None)

        if not app_meta:
            console.print(
                f"[bold red]Error:[/bold red] Application '{app_id}' not found in the public registry."
            )
            return

        namespace, name = app_id.split("/")
        version = app_meta["version"]
        tag = f"{namespace}-{name}-v{version}"
        asset_name = (
            f"{name}-v{version}.tar.gz"  # Assuming this is the new standard asset name
        )
        download_url = APPS_DOWNLOAD_URL_TEMPLATE.format(tag=tag, asset_name=asset_name)

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            archive_path = tmp_path / asset_name

            with console.status(f"Downloading {app_id}@{version}..."):
                async with httpx.AsyncClient(
                    follow_redirects=True, timeout=60.0
                ) as client:
                    response = await client.get(download_url)
                    response.raise_for_status()
                archive_path.write_bytes(response.content)

            with console.status("Unpacking application assets..."):
                with tarfile.open(archive_path, "r:gz") as tar:
                    tar.extractall(path=tmp_path)

            app_manifest_path = tmp_path / "app.cx.yaml"
            if not app_manifest_path.exists():
                console.print(
                    "[bold red]Error:[/bold red] Application package is invalid: missing 'app.cx.yaml'."
                )
                return

            with open(app_manifest_path, "r") as f:
                app_manifest = yaml.safe_load(f)

            with console.status("Resolving blueprint dependencies..."):
                blueprint_deps = app_manifest.get("dependencies", {}).get(
                    "blueprints", []
                )
                for blueprint_id in blueprint_deps:
                    try:
                        # Use to_thread to run the synchronous resolver method in an async context
                        await asyncio.to_thread(
                            self.resolver.load_blueprint_by_id, blueprint_id
                        )
                        console.print(
                            f"  [green]✓[/green] Resolved blueprint: [dim]{blueprint_id}[/dim]"
                        )
                    except Exception as e:
                        console.print(
                            f"[bold red]Failed to resolve blueprint dependency '{blueprint_id}': {e}[/bold red]"
                        )
                        return

            with console.status("Installing application assets..."):
                installed_assets = []
                for asset_type in ["flows", "queries", "scripts", "templates"]:
                    source_dir = tmp_path / asset_type
                    if source_dir.is_dir():
                        target_dir = CX_HOME / asset_type
                        target_dir.mkdir(exist_ok=True)
                        for item in source_dir.iterdir():
                            shutil.copy(item, target_dir)
                            installed_assets.append(f"{asset_type}/{item.name}")
                            console.print(
                                f"  [green]✓[/green] Installed asset: [dim]{asset_type}/{item.name}[/dim]"
                            )

            local_manifest["installed_apps"][app_id] = {
                "version": version,
                "assets": installed_assets,
                "dependencies": {"blueprints": blueprint_deps},
            }
            self._save_local_manifest(local_manifest)

            # --- Interactive Connection Setup ---
            if not no_interactive:
                # --- Just-in-Time Import and Instantiation to break circular dependency ---
                from .connection_manager import ConnectionManager

                connection_manager = ConnectionManager()

                required_conns = app_manifest.get("required_connections", [])
                if required_conns:
                    console.print(
                        "\n[bold]Application requires the following connections:[/bold]"
                    )
                    for conn_req in required_conns:
                        console.print(
                            f"\n--- Setting up connection: [bold cyan]{conn_req['id']}[/bold cyan] ---"
                        )
                        console.print(f"[dim]{conn_req['description']}[/dim]")
                        await connection_manager.create_interactive(
                            preselected_blueprint_id=conn_req["blueprint"]
                        )
                else:
                    console.print(
                        "No new connections are required for this application."
                    )

            console.print(
                f"\n[bold green]✓[/bold green] Successfully installed application [cyan]{app_id}@{version}[/cyan]."
            )

            readme_path = tmp_path / "README.md"
            if readme_path.exists():
                from rich.markdown import Markdown

                console.print("\n--- Application README ---")
                console.print(Markdown(readme_path.read_text()))

    # --- NEW: list_installed_apps and uninstall methods ---
    async def list_installed_apps(self):
        """Lists locally installed applications."""
        manifest = self._load_local_manifest()
        apps = manifest.get("installed_apps", {})
        if not apps:
            console.print("No applications are currently installed.")
            return

        table = Table(title="Locally Installed Applications")
        table.add_column("ID", style="cyan", no_wrap=True)
        table.add_column("Version", style="magenta")
        table.add_column("Asset Count", style="green", justify="right")

        for app_id, details in apps.items():
            table.add_row(
                app_id, details.get("version"), str(len(details.get("assets", [])))
            )

        console.print(table)

    async def uninstall(self, app_id: str):
        """Uninstalls an application and removes its assets."""
        manifest = self._load_local_manifest()
        app_to_remove = manifest.get("installed_apps", {}).get(app_id)

        if not app_to_remove:
            console.print(
                f"[bold red]Error:[/bold red] Application '{app_id}' is not installed."
            )
            return

        console.print(
            "The following assets will be [bold red]DELETED[/bold red] from your `~/.cx` directory:"
        )
        for asset in app_to_remove.get("assets", []):
            console.print(f"- {asset}")

        session = PromptSession()
        confirmed = await session.prompt_async(
            f"\nAre you sure you want to uninstall '{app_id}'? [y/n]: "
        )

        if confirmed.lower() == "y":
            with console.status(f"Uninstalling {app_id}..."):
                for asset_path_str in app_to_remove.get("assets", []):
                    full_path = CX_HOME / asset_path_str
                    if full_path.exists():
                        full_path.unlink()

                del manifest["installed_apps"][app_id]
                self._save_local_manifest(manifest)
            console.print(
                f"[bold green]✓[/bold green] Application '{app_id}' has been uninstalled."
            )
        else:
            console.print("[yellow]Uninstallation cancelled.[/yellow]")

    async def _get_registry(self) -> Dict[str, Any]:
        """Fetches and parses the public application/blueprint registry."""
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(APPS_REGISTRY_URL)
                response.raise_for_status()
            return yaml.safe_load(response.text)
        except Exception as e:
            console.print(
                f"[bold red]Error:[/bold red] Could not fetch or parse the application registry. {e}"
            )
            return {}

    async def get_available_blueprints(self) -> List[Dict[str, Any]]:
        """Returns a list of available blueprints from the public registry."""
        registry = await self._get_registry()
        # In the future, the registry will have a dedicated 'blueprints' key.
        # For now, we can infer them from the applications' dependencies.
        # Let's assume for now it has a top-level `blueprints` key for simplicity.
        return registry.get("blueprints", [])
