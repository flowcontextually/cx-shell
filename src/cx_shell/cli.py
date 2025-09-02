import asyncio
import shutil
from pathlib import Path
import sys
import structlog
import logging

import typer
from rich.console import Console

# --- Local Application Imports ---
from .engine.connector.cli import app as connector_app
from .engine.transformer.cli import app as transformer_app
from .engine.connector.service import ConnectorService
from .interactive.main import start_repl
# We do not need the utils here, so the import can be removed if not used elsewhere
# from .utils import get_asset_path


def setup_logging(verbose: bool):
    """
    Configures structlog for the entire application.
    - Default level: INFO (clean user output)
    - Verbose level: DEBUG (for power users)
    - All logs are routed to stderr to keep stdout clean for piping.
    """
    log_level = logging.DEBUG if verbose else logging.INFO

    # This is the definitive structlog configuration for a CLI tool
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.dev.ConsoleRenderer(),
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    # Get the root logger and configure its handler
    root_logger = logging.getLogger()
    handler = logging.StreamHandler(sys.stderr)
    # We don't need a formatter because ConsoleRenderer does it all
    root_logger.addHandler(handler)
    root_logger.setLevel(log_level)
    # Clear any other handlers that might have been added by libraries
    for handler in root_logger.handlers[:]:
        if handler.stream != sys.stderr:
            root_logger.removeHandler(handler)


# --- Centralized Path Constants ---
CX_HOME = Path.home() / ".cx"
DEFAULT_BLUEPRINTS_PATH = CX_HOME / "blueprints"


# --- Main Application Definition ---
app = typer.Typer(
    name="cx",
    help="""
    Welcome to the Contextual Shell!

    A declarative, multi-stage automation platform for modern data and ops teams.
    """,
    no_args_is_help=False,
    invoke_without_command=True,
    rich_markup_mode="markdown",
)


@app.callback(invoke_without_command=True)
def main_callback(
    ctx: typer.Context,
    verbose: bool = typer.Option(
        False, "--verbose", "-v", help="Enable verbose DEBUG logging."
    ),
):
    """
    The main callback for the cx command. Sets up logging and starts the REPL if no subcommand is invoked.
    """
    setup_logging(verbose)
    if ctx.invoked_subcommand is None:
        start_repl()


# --- Subcommand Groups ---
app.add_typer(
    connector_app,
    name="extract",
    help="Run Extraction workflows to fetch or send data.",
)

app.add_typer(
    transformer_app,
    name="transform",
    help="Run Transformation workflows to clean, shape, and format data.",
)


# --- Top-Level Commands ---


@app.command()
def init():
    """
    Initializes the cx shell environment in your home directory (~/.cx).

    Creates the necessary configuration and blueprint directories, and populates
    them with a sample project to get you started.
    """
    console = Console()

    # --- THIS IS THE FIX ---
    # The closing tag must match the opening tag exactly.
    console.print(
        "[bold green]Initializing Flow Contextually environment...[/bold green]"
    )
    # --- END FIX ---

    connections_dir = CX_HOME / "connections"
    secrets_dir = CX_HOME / "secrets"
    user_blueprints_dir = DEFAULT_BLUEPRINTS_PATH / "user"
    sample_blueprint_target_dir = (
        DEFAULT_BLUEPRINTS_PATH / "community" / "petstore" / "v2.0"
    )

    dirs_to_create = [
        connections_dir,
        secrets_dir,
        user_blueprints_dir,
        sample_blueprint_target_dir,
    ]

    for d in dirs_to_create:
        d.mkdir(parents=True, exist_ok=True)
        console.print(f"✅ Ensured directory exists: [dim]{d}[/dim]")

    fs_generic_conn = """
name: "Local Filesystem (Generic Root)"
id: "user:fs_generic"
api_catalog_id: "catalog:internal-filesystem"
auth_method_type: "none"
details:
  base_path: "/"
catalog:
  id: "catalog:internal-filesystem"
  name: "Local Filesystem"
  connector_provider_key: "fs-declarative"
"""

    petstore_conn = """
name: "Sample Petstore API"
id: "user:petstore"
api_catalog_id: "community/petstore@v2.0"
auth_method_type: "none"
"""

    smart_fetcher_conn = """
name: "System Smart Fetcher"
id: "user:system_smart_fetcher"
api_catalog_id: "catalog:internal-fetcher"
auth_method_type: "none"
catalog:
  id: "catalog:internal-fetcher"
  name: "Smart Fetcher"
  connector_provider_key: "internal-smart_fetcher"
"""

    python_sandbox_conn = """
name: "System Python Sandbox"
id: "user:system_python_sandbox"
api_catalog_id: "catalog:internal-python"
auth_method_type: "none"
details: {}
catalog:
  id: "catalog:internal-python"
  name: "Python Sandbox Runtime"
  connector_provider_key: "python-sandboxed"
"""

    files_to_write = {
        connections_dir / "fs_generic.conn.yaml": fs_generic_conn,
        connections_dir / "petstore.conn.yaml": petstore_conn,
        connections_dir / "system_smart_fetcher.conn.yaml": smart_fetcher_conn,
        connections_dir / "system_python_sandbox.conn.yaml": python_sandbox_conn,
    }

    for path, content in files_to_write.items():
        if not path.exists():
            path.write_text(content.strip())
            console.print(f"✅ Created sample connection: [dim]{path}[/dim]")
        else:
            console.print(f"☑️  File already exists, skipping: [dim]{path}[/dim]")

    try:
        source_assets_dir = Path(__file__).parent / "assets"
        petstore_blueprint_source = (
            source_assets_dir / "blueprints" / "community" / "petstore" / "v2.0"
        )

        if petstore_blueprint_source.is_dir():
            for file_path in petstore_blueprint_source.glob("*"):
                shutil.copy(file_path, sample_blueprint_target_dir)
            console.print(
                f"✅ Copied sample blueprint to: [dim]{sample_blueprint_target_dir}[/dim]"
            )
        else:
            console.print(
                f"[bold yellow]Warning:[/bold yellow] Could not find bundled sample blueprint at [dim]{petstore_blueprint_source}[/dim]. `connect user:petstore` may fail."
            )

    except Exception as e:
        console.print(f"[bold red]Error copying sample blueprint:[/bold red] {e}")

    console.print("\n[bold green]Initialization complete![/bold green]")
    console.print("Run `cx` to start the interactive shell and try the tutorial:")
    console.print("  1. `connect user:petstore --as api`")
    console.print("  2. `api.getPetById(petId=1)`")


@app.command()
def compile(
    spec_source: str = typer.Argument(
        ..., help="The path or URL to the OpenAPI/Swagger specification file."
    ),
    output_dir: Path = typer.Option(
        DEFAULT_BLUEPRINTS_PATH,
        "--output",
        "-o",
        help=f"The root directory to write the blueprint to. [default: {DEFAULT_BLUEPRINTS_PATH}]",
        file_okay=False,
        dir_okay=True,
        writable=True,
        resolve_path=True,
    ),
    name: str = typer.Option(
        ..., "--name", help="The machine-friendly name of the service (e.g., 'stripe')."
    ),
    version: str = typer.Option(
        "v1.0.0", "--version", help="The version for this blueprint package."
    ),
    namespace: str = typer.Option(
        "user",
        "--namespace",
        help="The target namespace (user, community, organization, system).",
    ),
):
    """
    Compiles an API specification into a Flow Contextually blueprint package.
    """
    console = Console(stderr=True)
    console.print(
        f"🚀 Starting compilation for service '[bold cyan]{name}[/bold cyan]'..."
    )

    full_output_dir = output_dir / namespace / name / version
    full_output_dir.mkdir(parents=True, exist_ok=True)
    console.print(f"Target directory prepared: [dim]{full_output_dir}[/dim]")

    script_input = {
        "spec_source": spec_source,
        "output_dir": str(full_output_dir),
    }

    compile_script_path = (
        Path(__file__).parent / "assets/system-tasks/compile.connector.yaml"
    )

    try:
        service = ConnectorService()
        result = asyncio.run(service.run_script(compile_script_path, script_input))

        for step_name, step_result in result.items():
            if isinstance(step_result, dict) and "error" in step_result:
                console.print(
                    "\n--- [bold red]❌ Compilation Workflow Failed[/bold red] ---"
                )
                console.print(
                    f"Error in step '[bold yellow]{step_name}[/bold yellow]':"
                )
                console.print(f"[red]{step_result['error']}[/red]")
                raise typer.Exit(code=1)

        console.print("\n--- [bold green]✅ Compilation Successful[/bold green] ---")
        console.print(
            f"Blueprint package for '[bold cyan]{name}@{version}[/bold cyan]' created at:"
        )
        console.print(f"[cyan]{full_output_dir}[/cyan]")

    except Exception as e:
        if not isinstance(e, typer.Exit):
            console.print(
                "\n--- [bold red]❌ Compilation Command Failed[/bold red] ---"
            )
            console.print(f"[red]An unexpected error occurred:[/red] {e}")
        raise typer.Exit(code=1)
