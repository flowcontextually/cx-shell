import asyncio
import shlex
import json
import ast
from typing import List, Dict, Any, Optional

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
    RunSqlQueryAction,
    BrowsePathAction,
    ReadContentAction,
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
        self.positional_arg_actions = {"query", "browse", "read"}

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
        Parses and executes a dot-notation command, dispatching to the correct
        parser based on the action verb.
        """
        try:
            alias, rest = command_text.split(".", 1)
            action_name, args_str = rest.split("(", 1)
            args_str = args_str.rstrip(")")
        except ValueError:
            console.print(
                "[bold red]Invalid syntax.[/bold red] Expected format: `alias.action(...)`"
            )
            return

        if alias not in self.state.connections:
            console.print(
                f"[bold red]Error:[/bold red] Unknown connection alias '{alias}'."
            )
            return

        connection_source = self.state.connections[alias]
        step = None

        status_text = f"Executing command on [cyan]{alias}[/cyan]..."
        with console.status(status_text, spinner="dots") as status:
            try:
                # Dispatch based on whether the action is a special positional verb
                if action_name in self.positional_arg_actions:
                    arg = args_str.strip().strip("'\"")
                    status.update(
                        f"Executing [cyan]{alias}[/cyan].[yellow]{action_name}[/yellow]([magenta]'{arg}'[/magenta])..."
                    )

                    if action_name == "query":
                        step = ConnectorStep(
                            id="interactive_query",
                            name="Interactive Query",
                            connection_source=connection_source,
                            run=RunSqlQueryAction(
                                action="run_sql_query", query=arg, parameters={}
                            ),
                        )
                    elif action_name == "browse":
                        step = ConnectorStep(
                            id="interactive_browse",
                            name="Interactive Browse",
                            connection_source=connection_source,
                            run=BrowsePathAction(action="browse_path", path=arg),
                        )
                    elif action_name == "read":
                        step = ConnectorStep(
                            id="interactive_read",
                            name="Interactive Read",
                            connection_source=connection_source,
                            run=ReadContentAction(action="read_content", path=arg),
                        )
                else:
                    # Fallback to robust keyword argument parsing for all other actions
                    action_context = self._parse_kwargs(args_str)
                    if action_context is None:
                        return

                    status.update(
                        f"Executing [cyan]{alias}[/cyan].[yellow]{action_name}[/yellow]([magenta]{action_context}[/magenta])..."
                    )
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

                if not step:
                    console.print(
                        f"[bold red]Error:[/bold red] Could not construct an action for '{action_name}'."
                    )
                    return

                script = ConnectorScript(name="Interactive Script", steps=[step])
                results = await self.service.engine.run_script_model(script)

                status.stop()

                output = results.get(step.name, {"error": "No result returned."})
                if "error" in output:
                    console.print(
                        f"[bold red]Runtime Error:[/bold red] {output['error']}"
                    )
                    return

                final_output = output
                if (
                    action_name == "query"
                    and isinstance(output, dict)
                    and "data" in output
                ):
                    final_output = output["data"]
                elif (
                    action_name == "read"
                    and isinstance(output, dict)
                    and "content" in output
                ):
                    console.print(output["content"])
                    return

                formatted_json = json.dumps(final_output, indent=2, default=str)
                syntax = Syntax(
                    formatted_json, "json", theme="monokai", line_numbers=True
                )
                console.print(syntax)

            except Exception as e:
                status.stop()
                console.print(f"[bold red]Engine Error:[/bold red] {e}")

    def _parse_kwargs(self, args_str: str) -> Optional[Dict[str, Any]]:
        """Helper function to parse keyword arguments using ast."""
        action_context: Dict[str, Any] = {}
        if args_str.strip():
            try:
                # Use `ast.parse` to handle a full function call, but only extract keywords
                wrapper_expression = f"f({args_str})"
                parsed_ast = ast.parse(wrapper_expression, mode="eval")
                call_node = parsed_ast.body
                if not isinstance(call_node, ast.Call) or call_node.args:
                    raise TypeError(
                        "Only key=value arguments are supported for this action."
                    )
                action_context = {
                    kw.arg: ast.literal_eval(kw.value) for kw in call_node.keywords
                }
            except (ValueError, SyntaxError, TypeError) as e:
                console.print(
                    f"[bold red]Argument Error:[/bold red] Could not parse arguments '{args_str}'. Error: {e}"
                )
                return None
        return action_context

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
        """
        Executes the 'connect' command in the interactive shell.

        It parses the user's input, calls the ConnectorService to test the
        connection, and provides clear, user-friendly feedback, including a
        spinner during the connection attempt.
        """
        if len(args) < 3 or args[1].lower() != "--as":
            console.print(
                "[bold red]Invalid syntax.[/bold red] Use: `connect <connection_source> --as <alias>`"
            )
            return

        source = args[0]
        alias = args[2]

        # Use a status spinner for improved user experience during the network call.
        status = Status(
            f"Attempting to connect to '[yellow]{source}[/yellow]'...", spinner="dots"
        )
        with status:
            # The `test_connection` service is guaranteed to not raise an exception.
            result = await self.service.test_connection(source)

        # Check the status from the returned dictionary.
        if result.get("status") == "success":
            self.state.connections[alias] = source
            console.print(
                f"[bold green]✅ Connection successful.[/bold green] Alias '[cyan]{alias}[/cyan]' is now active."
            )
        else:
            # Display the clean error message provided by the service.
            error_message = result.get("message", "An unknown error occurred.")
            console.print(f"[bold red]❌ Connection failed:[/bold red] {error_message}")
