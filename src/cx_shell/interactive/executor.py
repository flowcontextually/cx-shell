import asyncio
import shlex
import json
import ast  # Abstract Syntax Tree library for safe parsing of arguments
from typing import List, Dict, Any

from rich.console import Console
from rich.syntax import Syntax

from ..engine.connector.service import ConnectorService
from .session import SessionState
from cx_core_schemas.connector_script import (
    ConnectorScript,
    ConnectorStep,
    RunDeclarativeAction,
)

# Use a separate console for rich, formatted output in the REPL
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
            # --- Primary Dispatcher Logic ---
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
            args_str = args_str.rstrip(")")  # Remove trailing parenthesis
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

        # --- Robust Argument Parsing using Abstract Syntax Trees ---
        action_context: Dict[str, Any] = {}
        if args_str.strip():
            try:
                # `ast.parse` is the correct, safe way to handle keyword arguments.
                # We wrap the user's input in a dummy function call `f(...)` to create
                # a valid expression for the parser.
                wrapper_expression = f"f({args_str})"
                parsed_ast = ast.parse(wrapper_expression, mode="eval")

                # The body of the parsed expression will be a `Call` node.
                call_node = parsed_ast.body
                if not isinstance(call_node, ast.Call):
                    raise SyntaxError("Input must be valid function call arguments.")

                # We only support keyword arguments for clarity and safety.
                if call_node.args:
                    raise TypeError(
                        "Positional arguments are not supported. Use key=value pairs."
                    )

                # Safely evaluate the value of each keyword argument.
                action_context = {
                    kw.arg: ast.literal_eval(kw.value) for kw in call_node.keywords
                }
            except (ValueError, SyntaxError, TypeError, KeyError) as e:
                console.print(
                    f"[bold red]Argument Error:[/bold red] Could not parse arguments '{args_str}'. Ensure they are valid key=value pairs (e.g., petId=1, status='available'). Error: {e}"
                )
                return

        console.print(
            f"Executing [cyan]{alias}[/cyan].[yellow]{action_name}[/yellow]([magenta]{action_context}[/magenta])..."
        )

        # Dynamically construct the workflow step. This makes the executor
        # generic and driven entirely by the loaded blueprint.
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

        try:
            results = await self.service.engine.run_script_model(script)
            output = results.get(step.name, {"error": "No result returned from step."})

            if "error" in output:
                console.print(f"[bold red]Runtime Error:[/bold red] {output['error']}")
                return

            # Pretty-print any JSON-like output using Rich's Syntax.
            formatted_json = json.dumps(output, indent=2, default=str)
            syntax = Syntax(formatted_json, "json", theme="monokai", line_numbers=True)
            console.print(syntax)

        except Exception as e:
            console.print(f"[bold red]Engine Error:[/bold red] {e}")

    # --- Built-in Shell Commands ---

    def execute_help(self, args: List[str]):
        """Displays help information."""
        console.print("\n[bold]Contextual Shell Help[/bold]")
        console.print("-----------------------")
        console.print("[bold cyan]Built-in Commands:[/bold cyan]")
        console.print(
            "  [yellow]connect[/yellow] <source> --as <alias>   - Test and activate a connection."
        )
        console.print(
            "  [yellow]connections[/yellow]                       - List active connections in the session."
        )
        console.print(
            "  [yellow]help[/yellow]                            - Show this help message."
        )
        console.print(
            "  [yellow]exit[/yellow] | [yellow]quit[/yellow]                      - Exit the shell."
        )
        console.print("\n[bold cyan]Dynamic Actions (from Blueprints):[/bold cyan]")
        console.print("  <alias>.<action_name>(key=value, ...)")
        console.print(
            "\nExample: `connect user:petstore --as api` followed by `api.getPetById(petId=1)`\n"
        )

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

        console.print(
            f"Attempting to connect to '[yellow]{source}[/yellow]' with alias '[cyan]{alias}[/cyan]'..."
        )

        result = await self.service.test_connection(source)

        if result.get("status") == "success":
            self.state.connections[alias] = source
            console.print(
                f"[bold green]✅ Connection successful.[/bold green] Alias '[cyan]{alias}[/cyan]' is now active."
            )
        else:
            console.print(
                f"[bold red]❌ Connection failed:[/bold red] {result.get('message', 'Unknown error')}"
            )
