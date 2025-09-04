import asyncio
import functools
import shutil
from pathlib import Path
import sys
from typing import List, Optional
import structlog
import logging

import typer
from rich.console import Console
from rich.traceback import Traceback

# --- Local Application Imports ---
from cx_shell.engine.connector.cli import app as connector_app
from cx_shell.engine.transformer.cli import app as transformer_app
from cx_shell.engine.connector.service import ConnectorService
from cx_shell.interactive.main import start_repl
from cx_shell.engine.connector.config import CX_HOME, BLUEPRINTS_BASE_PATH
from cx_shell.management.connection_manager import ConnectionManager
from cx_shell.management.app_manager import AppManager
from cx_shell.management.flow_manager import FlowManager
from cx_shell.management.query_manager import QueryManager
from cx_shell.management.script_manager import ScriptManager
from cx_shell.state import APP_STATE

# from cx_shell.utils import get_asset_path # Assuming you create this file
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


def handle_exceptions(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        console = Console(stderr=True)
        try:
            return func(*args, **kwargs)
        except typer.Exit:
            raise
        except Exception as e:
            console.print(f"[bold red]Error:[/bold red] {e}")
            # Read from the central state object
            if APP_STATE.verbose_mode:
                console.print(
                    Traceback.from_exception(
                        type(e), e, e.__traceback__, show_locals=True
                    )
                )
            raise typer.Exit(code=1)

    return wrapper


# --- Main Application Definition ---
app = typer.Typer(
    name="cx",
    help="Welcome to the Contextual Shell!\n\nA declarative, multi-stage automation platform.",
    no_args_is_help=False,
    invoke_without_command=True,
    rich_markup_mode="markdown",
)

connection_app = typer.Typer(
    name="connection", help="Manage your local connections.", no_args_is_help=True
)

app_app = typer.Typer(
    name="app",
    help="Discover, install, and manage Contextually Applications.",
    no_args_is_help=True,
)
flow_app = typer.Typer(
    name="flow",
    help="List and manage reusable .flow.yaml workflows.",
    no_args_is_help=True,
)
query_app = typer.Typer(
    name="query", help="List and manage reusable .sql queries.", no_args_is_help=True
)
script_app = typer.Typer(
    name="script", help="List and manage reusable .py scripts.", no_args_is_help=True
)

app.add_typer(app_app, name="app")
app.add_typer(flow_app, name="flow")
app.add_typer(query_app, name="query")
app.add_typer(script_app, name="script")


@app.callback(invoke_without_command=True)
def main_callback(
    ctx: typer.Context,
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help="Enable verbose DEBUG logging for detailed tracebacks.",
    ),
):
    APP_STATE.verbose_mode = verbose
    setup_logging(verbose)
    if ctx.invoked_subcommand is None:
        start_repl()


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

app.add_typer(connection_app, name="connection")


# --- Top-Level Commands ---


@app.command()
@handle_exceptions
def init():
    """
    Initializes the cx shell environment in your home directory (~/.cx).

    Creates the necessary configuration and blueprint directories, and populates
    them with a sample project to get you started.
    """
    console = Console()

    console.print(
        "[bold green]Initializing Flow Contextually environment...[/bold green]"
    )

    connections_dir = CX_HOME / "connections"
    secrets_dir = CX_HOME / "secrets"
    user_blueprints_dir = BLUEPRINTS_BASE_PATH / "user"
    flows_dir = CX_HOME / "flows"
    queries_dir = CX_HOME / "queries"
    scripts_dir = CX_HOME / "scripts"
    sample_blueprint_target_dir = (
        BLUEPRINTS_BASE_PATH / "community" / "github" / "v0.1.0"
    )

    dirs_to_create = [
        connections_dir,
        secrets_dir,
        user_blueprints_dir,
        flows_dir,
        queries_dir,
        scripts_dir,
        sample_blueprint_target_dir,
    ]

    for d in dirs_to_create:
        d.mkdir(parents=True, exist_ok=True)
        console.print(f"✅ Ensured directory exists: [dim]{d}[/dim]")

    github_conn = """
name: "GitHub Public API"
id: "user:github"
api_catalog_id: "community/github@v0.1.0"
auth_method_type: "none"
"""

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
        connections_dir / "github.conn.yaml": github_conn,
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
        github_blueprint_source = (
            source_assets_dir / "blueprints" / "community" / "github" / "v0.1.0"
        )

        if github_blueprint_source.is_dir():
            shutil.copytree(
                github_blueprint_source, sample_blueprint_target_dir, dirs_exist_ok=True
            )
            console.print(
                f"✅ Copied sample blueprint to: [dim]{sample_blueprint_target_dir}[/dim]"
            )
        else:
            console.print(
                f"[bold yellow]Warning:[/bold yellow] Could not find bundled sample blueprint at [dim]{github_blueprint_source}[/dim]. `connect user:github` may fail."
            )
    except Exception as e:
        console.print(f"[bold red]Error copying sample blueprint:[/bold red] {e}")

    console.print("\n[bold green]Initialization complete![/bold green]")
    console.print("Run `cx` to start the interactive shell and try the new tutorial:")
    console.print("  1. `connect user:github --as gh`")
    console.print('  2. `gh.getUser(username="torvalds")`')


@app.command()
@handle_exceptions
def compile(
    spec_source: str = typer.Argument(
        ..., help="The path or URL to the OpenAPI/Swagger specification file."
    ),
    output_dir: Path = typer.Option(
        BLUEPRINTS_BASE_PATH,
        "--output",
        "-o",
        help=f"The root directory to write the blueprint to. [default: {BLUEPRINTS_BASE_PATH}]",
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


@connection_app.command("list")
@handle_exceptions
def connection_list():
    """Lists all locally configured connections."""
    manager = ConnectionManager()
    manager.list_connections()


@connection_app.command("create")
@handle_exceptions
def connection_create(
    # The blueprint is now a standard option that can guide interactive mode.
    blueprint: Optional[str] = typer.Option(
        None, "--blueprint", "-b", help="Pre-select the blueprint ID to use."
    ),
    name: Optional[str] = typer.Option(
        None, "--name", help="Connection name (for non-interactive mode)."
    ),
    id: Optional[str] = typer.Option(
        None, "--id", help="Connection ID/alias (for non-interactive mode)."
    ),
    detail: Optional[List[str]] = typer.Option(
        None,
        "--detail",
        help="A non-sensitive key=value pair. e.g., 'server=localhost'",
    ),
    secret: Optional[List[str]] = typer.Option(
        None, "--secret", help="A SENSITIVE key=value pair. e.g., 'password=123'"
    ),
):
    """
    Creates a new connection configuration.

    - Run without flags for a fully guided setup.
    - Run with `--blueprint` to start a guided setup for a specific blueprint.
    - Provide `--name`, `--id`, and `--blueprint` for non-interactive creation.
    """
    console = Console()
    manager = ConnectionManager()

    # Determine if we are in fully non-interactive (scriptable) mode.
    is_fully_non_interactive = all([name, id, blueprint])

    try:
        if is_fully_non_interactive:
            # --- Non-Interactive Mode ---
            details_dict = dict(item.split("=", 1) for item in detail) if detail else {}
            secrets_dict = dict(item.split("=", 1) for item in secret) if secret else {}
            manager.create_non_interactive(
                name=name,
                id=id,
                blueprint_id=blueprint,
                details=details_dict,
                secrets=secrets_dict,
            )
        else:
            # --- Interactive / Semi-Interactive Mode ---
            if detail or secret:
                console.print(
                    "[bold red]Error:[/bold red] --detail and --secret flags can only be used in non-interactive mode (when --name, --id, and --blueprint are all provided)."
                )
                raise typer.Exit(code=1)

            # Pass the pre-selected blueprint ID if the user provided it.
            manager.create_interactive(preselected_blueprint_id=blueprint)

    except Exception as e:
        console.print(f"\n[bold red]An unexpected error occurred:[/bold red] {e}")
        raise typer.Exit(code=1)


@app_app.command("search")
@handle_exceptions
def app_search(
    query: str = typer.Argument(
        None, help="Optional search query to filter applications."
    ),
):
    """Searches the public application registry."""
    manager = AppManager()
    asyncio.run(manager.search(query))


@app_app.command("install")
@handle_exceptions
def app_install(
    app_id: str = typer.Argument(
        ...,
        help="The ID of the application to install (e.g., official/github-repo-manager).",
    ),
):
    """Installs an application from the public registry."""
    manager = AppManager()
    asyncio.run(manager.install(app_id))


@app_app.command("list")
@handle_exceptions
def app_list():
    """Lists all locally installed applications."""
    manager = AppManager()
    asyncio.run(manager.list_installed_apps())


@app_app.command("uninstall")
@handle_exceptions
def app_uninstall(
    app_id: str = typer.Argument(
        ...,
        help="The ID of the application to uninstall (e.g., official/github-repo-manager).",
    ),
):
    """Uninstalls an application and removes its assets."""
    manager = AppManager()
    asyncio.run(manager.uninstall(app_id))


@flow_app.command("list")
@handle_exceptions
def flow_list():
    """Lists all locally saved .flow.yaml workflows."""
    manager = FlowManager()
    manager.list_flows()


@query_app.command("list")
@handle_exceptions
def query_list():
    """Lists all locally saved .sql queries."""
    manager = QueryManager()
    manager.list_queries()


@script_app.command("list")
@handle_exceptions
def script_list():
    """Lists all locally saved .py scripts."""
    manager = ScriptManager()
    manager.list_scripts()
