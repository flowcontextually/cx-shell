import yaml
from typing import Dict, Optional

from rich.console import Console
from rich.prompt import Prompt, Confirm
from rich.table import Table

from ..engine.connector.config import ConnectionResolver, CX_HOME

# Use a single, shared console for all rich output.
console = Console()


class ConnectionManager:
    """A service for managing local connection configurations."""

    def __init__(self):
        self.resolver = ConnectionResolver()
        self.connections_dir = CX_HOME / "connections"
        self.secrets_dir = CX_HOME / "secrets"
        self.connections_dir.mkdir(exist_ok=True)
        self.secrets_dir.mkdir(exist_ok=True)

    def list_connections(self):
        """Lists all locally configured connections."""
        if not any(self.connections_dir.iterdir()):
            console.print(
                "No connections found. Create one with `cx connection create`."
            )
            return

        table = Table(title="Local Connections")
        table.add_column("Name", style="cyan")
        table.add_column("ID", style="green")
        table.add_column("Blueprint ID", style="magenta")

        for conn_file in sorted(self.connections_dir.glob("*.conn.yaml")):
            try:
                with open(conn_file, "r") as f:
                    data = yaml.safe_load(f)
                    conn_id = data.get("id", "user:N/A").split(":", 1)[1]
                    table.add_row(
                        data.get("name", "N/A"),
                        conn_id,
                        data.get("api_catalog_id", "N/A"),
                    )
            except Exception:
                table.add_row(f"[red]Error parsing: {conn_file.name}[/red]", "", "")

        console.print(table)

    def create_interactive(self, preselected_blueprint_id: Optional[str] = None):
        """
        Interactively creates a new connection by first loading a blueprint
        to determine the required fields.
        """
        console.print(
            "[bold green]--- Create a New Connection (Interactive) ---[/bold green]"
        )

        blueprint_id = preselected_blueprint_id or Prompt.ask(
            "Enter the Blueprint ID to use (e.g., system/mssql@v0.1.0)"
        )

        # --- Blueprint Loading & On-Demand Sync ---
        status_text = (
            f"Loading blueprint [bold magenta]{blueprint_id}[/bold magenta]..."
        )
        with console.status(status_text, spinner="dots"):
            try:
                # This single, clean call to the public API of the resolver
                # correctly encapsulates the git sync, file loading, and validation logic.
                catalog = self.resolver.load_blueprint_by_id(blueprint_id)
                auth_methods = catalog.supported_auth_methods

                if not auth_methods:
                    raise ValueError(
                        "Blueprint does not define any `supported_auth_methods`."
                    )

            except Exception as e:
                console.print(
                    f"\n[bold red]Error:[/bold red] Could not load blueprint '{blueprint_id}'."
                )
                console.print(f"[dim]Details: {e}[/dim]")
                return

        # --- Interactive Prompting Driven by Blueprint ---
        chosen_method = auth_methods[0]
        if len(auth_methods) > 1:
            console.print("\n[bold]Select an authentication method:[/bold]")
            choices = {str(i + 1): method for i, method in enumerate(auth_methods)}
            for i, method in choices.items():
                console.print(f"  [cyan]{i}[/cyan]: {method.display_name}")
            choice_str = Prompt.ask(
                "Enter your choice", choices=list(choices.keys()), default="1"
            )
            chosen_method = choices[choice_str]

        console.print(
            f"\nPlease provide the following details for '[yellow]{chosen_method.display_name}[/yellow]':"
        )
        conn_name = Prompt.ask("Enter a friendly name for this connection")
        conn_id = Prompt.ask(
            "Enter a unique ID (alias)", default=conn_name.lower().replace(" ", "-")
        )

        details, secrets = {}, {}
        for field in chosen_method.fields:
            value = Prompt.ask(field.label, password=field.is_password)
            if field.type == "secret":
                secrets[field.name] = value
            else:
                details[field.name] = value

        # --- File Creation ---
        conn_content = {
            "name": conn_name,
            "id": f"user:{conn_id}",
            "api_catalog_id": blueprint_id,
            "auth_method_type": chosen_method.type,
            "details": details,
        }
        secrets_content = "\n".join(
            [f"{key.upper()}={value}" for key, value in secrets.items()]
        )
        conn_file = self.connections_dir / f"{conn_id}.conn.yaml"
        secret_file = self.secrets_dir / f"{conn_id}.secret.env"

        console.print("\n[bold]Configuration to be saved:[/bold]")
        console.print(yaml.dump(conn_content, sort_keys=False))

        if Confirm.ask("\nDo you want to save this connection?", default=True):
            conn_file.write_text(yaml.dump(conn_content, sort_keys=False))
            secret_file.write_text(secrets_content)
            console.print(
                f"\n[bold green]✅ Connection '{conn_name}' saved successfully![/bold green]"
            )
        else:
            console.print("\n[bold yellow]Aborted.[/bold yellow]")

    def create_non_interactive(
        self, name: str, id: str, blueprint_id: str, details: Dict, secrets: Dict
    ):
        """Creates a connection non-interactively from provided arguments."""
        console.print(
            f"[bold green]--- Creating Connection '{name}' (Non-Interactive) ---[/bold green]"
        )

        # In a future version, this would also load the blueprint to validate the provided fields.
        # For now, we trust the user has provided the correct details and secrets.
        conn_content = {
            "name": name,
            "id": f"user:{id}",
            "api_catalog_id": blueprint_id,
            "auth_method_type": "credentials",  # A reasonable default, may need to be specified in the future.
            "details": details,
        }
        secrets_content = "\n".join(
            [f"{key.upper()}={value}" for key, value in secrets.items()]
        )

        conn_file = self.connections_dir / f"{id}.conn.yaml"
        secret_file = self.secrets_dir / f"{id}.secret.env"

        conn_file.write_text(yaml.dump(conn_content, sort_keys=False))
        secret_file.write_text(secrets_content)

        console.print(
            f"✅ Connection '{name}' saved successfully to [dim]{conn_file}[/dim]"
        )
