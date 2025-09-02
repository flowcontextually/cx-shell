import asyncio
import shlex
import json
import ast
from typing import List, Dict, Any

from rich.console import Console
from rich.syntax import Syntax
from rich.status import Status
from rich.panel import Panel
from rich.text import Text
from rich.table import Table

from ..engine.connector.service import ConnectorService
from .session import SessionState
from cx_core_schemas.connector_script import (
    ConnectorScript,
    ConnectorStep,
    RunDeclarativeAction,
)

# Use a single, shared console for all rich output in the REPL
console = Console()


class CommandExecutor:
    """
    Parses and executes commands entered in the interactive shell.

    This class acts as the primary dispatcher, routing user input to either
    built-in shell commands (like 'connect') or to the dot-notation parser
    for dynamic actions against active, blueprint-driven connections.
    """

    def __init__(self, state: SessionState):
        self.state = state
        self.service = ConnectorService()
        self.builtin_commands = {
            "connect": self.execute_connect,
            "connections": self.execute_list_connections,
            "help": self.execute_help,
        }

    async def execute(self, command_text: str):
        """The main entry point for parsing and executing any user command."""
        try:
            if (
                "." in command_text
                and "(" in command_text
                and command_text.endswith(")")
            ):
                await self.execute_dot_notation(command_text)
                return

            parts = shlex.split(command_text)
            if not parts:
                return

            command = parts[0].lower()
            args = parts[1:]

            if command in self.builtin_commands:
                handler = self.builtin_commands[command]
                if asyncio.iscoroutinefunction(handler):
                    await handler(args)
                else:
                    handler(args)
            else:
                console.print(
                    f"[bold red]Error:[/bold red] Unknown command '{command}'. Type 'help' for a list of commands."
                )

        except Exception as e:
            console.print(f"[bold red]Execution Error:[/bold red] {e}")

    async def execute_dot_notation(self, command_text: str):
        """
        Parses and executes a dot-notation command like 'alias.action(key=value)'.
        """
        try:
            alias, rest = command_text.split(".", 1)
            action_name, args_str = rest.split("(", 1)
            args_str = args_str.rstrip(")")
        except ValueError:
            console.print(
                "[bold red]Invalid syntax.[/bold red] Expected format: `alias.action(arguments)`"
            )
            return

        if alias not in self.state.connections:
            console.print(
                f"[bold red]Error:[/bold red] Unknown connection alias '{alias}'. Use 'connections' to see active aliases."
            )
            return

        connection_source = self.state.connections[alias]

        action_context: Dict[str, Any] = {}
        if args_str.strip():
            try:
                wrapper_expression = f"f({args_str})"
                parsed_ast = ast.parse(wrapper_expression, mode="eval")
                call_node = parsed_ast.body
                if not isinstance(call_node, ast.Call) or call_node.args:
                    raise TypeError("Only key=value arguments are supported.")
                action_context = {
                    kw.arg: ast.literal_eval(kw.value) for kw in call_node.keywords
                }
            except (ValueError, SyntaxError, TypeError) as e:
                console.print(
                    f"[bold red]Argument Error:[/bold red] Could not parse arguments. {e}"
                )
                return

        step = ConnectorStep(
            id=f"interactive_{action_name}",
            name=f"Interactive {action_name}",
            connection_source=connection_source,
            run=RunDeclarativeAction(
                action="run_declarative_action",
                template_key=action_name,
                context=action_context,
            ),
        )
        script = ConnectorScript(name="Interactive Script", steps=[step])

        status = Status(
            f"Executing [cyan]{alias}[/cyan].[yellow]{action_name}[/yellow]...",
            spinner="dots",
        )
        with status:
            try:
                results = await self.service.engine.run_script_model(script)
                status.stop()
                output = results.get(
                    step.name, {"error": "No result returned from step."}
                )
                if "error" in output:
                    console.print(
                        f"[bold red]Runtime Error:[/bold red] {output['error']}"
                    )
                    return
                formatted_json = json.dumps(output, indent=2, default=str)
                syntax = Syntax(
                    formatted_json, "json", theme="monokai", line_numbers=True
                )
                console.print(syntax)
            except Exception as e:
                status.stop()
                console.print(f"[bold red]Engine Error:[/bold red] {e}")

    # --- Built-in Shell Commands ---

    def execute_help(self, args: List[str]):
        """Displays a structured and helpful guide for the interactive shell."""

        console.print()
        title = Panel(
            "[bold yellow]Welcome to the Contextual Shell (`cx`)[/bold yellow]",
            expand=False,
        )
        console.print(title)

        console.print(
            "\n`cx` is an interactive shell for the Contextually platform. It allows you to connect to APIs and data sources using pre-built 'blueprints' and execute actions dynamically."
        )

        # --- Built-in Commands Table ---
        builtins_table = Table(
            title="[bold cyan]Built-in Shell Commands[/bold cyan]",
            box=None,
            padding=(0, 1),
        )
        builtins_table.add_column("Command", style="yellow", no_wrap=True)
        builtins_table.add_column("Description")

        builtins_table.add_row(
            "connect <source> --as <alias>",
            "Test and activate a connection for the current session.",
        )
        builtins_table.add_row(
            "connections", "List all active connections in the current session."
        )
        builtins_table.add_row("help", "Show this help message.")
        builtins_table.add_row("exit | quit", "Exit the interactive shell.")

        console.print(builtins_table)

        # --- Dynamic Actions Explanation ---
        actions_table = Table(
            title="[bold cyan]Dynamic Blueprint Actions[/bold cyan]",
            box=None,
            padding=(0, 1),
        )
        actions_table.add_column("Syntax", style="yellow", no_wrap=True)
        actions_table.add_column("Description")

        actions_table.add_row(
            "<alias>.<action>(key=value, ...)",
            "Execute a blueprint-defined action on an active connection.",
        )

        console.print(actions_table)

        # --- Example Workflow ---
        console.print("[bold]Example Workflow:[/bold]")
        example = Text.from_markup("""
  1. Initialize your environment (first time only):
     [dim]$ cx init[/dim]
  2. Start the interactive shell:
     [dim]$ cx[/dim]
  3. Connect to the sample API:
     [dim]cx> connect user:petstore --as api[/dim]
  4. Discover and run an action:
     [dim]cx> api.<TAB>[/dim]
     [dim]cx> api.getPetById(petId=1)[/dim]
        """)
        console.print(
            Panel(example, title="[green]Getting Started[/green]", border_style="green")
        )
        console.print()

    def execute_list_connections(self, args: List[str]):
        """Lists the currently active connections in the session."""
        if not self.state.connections:
            console.print("No active connections in this session.")
            return

        console.print("\n[bold green]Active Connections:[/bold green]")
        for alias, source in self.state.connections.items():
            console.print(f"  - [cyan]{alias}[/cyan] -> [dim]{source}[/dim]")
        console.print()

    async def execute_connect(self, args: List[str]):
        """Executes the 'connect' command."""
        if len(args) < 3 or args[1].lower() != "--as":
            console.print(
                "[bold red]Invalid syntax.[/bold red] Use: `connect <connection_source> --as <alias>`"
            )
            return

        source = args[0]
        alias = args[2]

        status = Status(
            f"Attempting to connect to '[yellow]{source}[/yellow]'...", spinner="dots"
        )
        result = None
        with status:
            try:
                result = await self.service.test_connection(source)
            except Exception as e:
                status.stop()
                console.print(
                    f"[bold red]❌ Connection failed:[/bold red] An unexpected error occurred: {e}"
                )
                return

        if result and result.get("status") == "success":
            self.state.connections[alias] = source
            console.print(
                f"[bold green]✅ Connection successful.[/bold green] Alias '[cyan]{alias}[/cyan]' is now active."
            )
        else:
            error_message = (
                result.get("message", "Unknown error")
                if result
                else "Test returned no result."
            )
            console.print(f"[bold red]❌ Connection failed:[/bold red] {error_message}")
