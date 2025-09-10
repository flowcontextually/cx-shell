# /home/dpwanjala/repositories/cx-shell/src/cx_shell/interactive/output_handler.py

import json
from abc import ABC, abstractmethod
from typing import Any, Optional, Dict

import jmespath
from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.pretty import Pretty
from rich.syntax import Syntax
from rich.table import Table

# Import the command classes to identify them
from .commands import (
    Command,
    ConnectionCommand,
    FlowCommand,
    InspectCommand,
    QueryCommand,
    ScriptCommand,
    SessionCommand,
    VariableCommand,
)

# A single, shared console instance for all rich output in the REPL
console = Console()


class IOutputHandler(ABC):
    """
    An abstract interface for handling the output of executed commands.
    This architecture decouples the CommandExecutor from the presentation layer,
    allowing different interfaces (like a terminal or a WebSocket) to render
    the same command result in different ways.
    """

    @abstractmethod
    async def handle_result(
        self, result: Any, executable: Command, options: Optional[Dict] = None
    ):
        """
        Processes and displays the final result of a command.

        Args:
            result: The raw data returned by the command's execution.
            executable: The parsed command object itself, used to understand context.
            options: A dictionary of universal formatter options (e.g., from --cx-output).
        """
        pass


class RichConsoleHandler(IOutputHandler):
    """
    An implementation of the output handler that renders results to the
    terminal using the rich library. This class contains all the presentation
    logic for the interactive REPL.
    """

    async def handle_result(
        self, result: Any, executable: Command, options: Optional[Dict] = None
    ):
        """
        Processes and displays the final result to the console, intelligently
        selecting the best format (table, JSON, panel, or simple string).
        """
        if result is None:
            return

        options = options or {}

        # --- Stage 1: Handle Terminal Cases & Special Command Types ---

        # Handle simple string confirmation messages
        if isinstance(result, str):
            console.print(f"[bold green]✓[/bold green] {result}")
            return

        # Handle errors that have propagated through the execution chain
        if isinstance(result, dict) and "error" in result:
            console.print(f"[bold red]Runtime Error:[/bold red] {result['error']}")
            return

        # Handle the special panel format for the 'inspect' command
        if isinstance(executable, InspectCommand):
            summary = result
            panel_content = (
                f"[bold]Variable:[/bold] [cyan]{summary['var_name']}[/cyan]\n"
            )
            panel_content += f"[bold]Type:[/bold] [green]{summary['type']}[/green]\n"
            if "length" in summary:
                panel_content += f"[bold]Length:[/bold] {summary['length']}\n"
            if "keys" in summary:
                panel_content += f"[bold]Keys:[/bold] {summary['keys']}"
            if "item_zero_keys" in summary:
                panel_content += (
                    f"[bold]Item[0] Keys:[/bold] {summary['item_zero_keys']}"
                )
            console.print(
                Panel(panel_content, title="Object Inspector", border_style="yellow")
            )
            return

        # --- STAGE 2: DATA PROCESSING ---
        data_to_render = result
        is_list_command = (
            hasattr(executable, "subcommand") and executable.subcommand == "list"
        )

        # For non-list commands, process the data through formatters
        if not is_list_command:
            if options.get("query"):
                try:
                    data_to_render = jmespath.search(options["query"], result)
                except Exception as e:
                    console.print(f"[bold red]JMESPath Error:[/bold red] {e}")
                    return
            else:
                if isinstance(data_to_render, dict):
                    if "results" in data_to_render:
                        data_to_render = data_to_render["results"]
                    elif "data" in data_to_render:
                        data_to_render = data_to_render["data"]
                    elif "content" in data_to_render:
                        try:
                            data_to_render = json.loads(data_to_render["content"])
                        except (json.JSONDecodeError, TypeError):
                            pass

        # --- Stage 3: Output Rendering ---
        output_mode = options.get("output_mode", "default")

        # Condition for rendering a table
        is_list_of_dicts = (
            isinstance(data_to_render, list)
            and bool(data_to_render)
            and all(isinstance(i, dict) for i in data_to_render)
        )

        if output_mode == "table" or (
            is_list_command and is_list_of_dicts and not options
        ):
            if is_list_of_dicts:
                # Dynamically set table title based on the command type
                title = "Data View"
                if isinstance(executable, ConnectionCommand):
                    title = "Local Connections"
                elif isinstance(executable, FlowCommand):
                    title = "Available Flows"
                elif isinstance(executable, QueryCommand):
                    title = "Available Queries"
                elif isinstance(executable, ScriptCommand):
                    title = "Available Scripts"
                elif isinstance(executable, SessionCommand):
                    title = "Saved Sessions"
                elif isinstance(executable, VariableCommand):
                    title = "Session Variables"

                try:
                    table = Table(
                        title=f"[bold]{title}[/bold]", box=box.ROUNDED, show_lines=True
                    )
                    headers = options.get("columns") or list(data_to_render[0].keys())
                    for header in headers:
                        table.add_column(str(header), style="cyan", overflow="fold")
                    for row in data_to_render:
                        table.add_row(*(str(row.get(h, "")) for h in headers))
                    console.print(table)
                    return
                except Exception:
                    # Fallback to JSON on any table rendering error
                    pass

        # Default to printing pretty JSON or a simple representation for all other cases
        try:
            if data_to_render is None or not isinstance(data_to_render, (dict, list)):
                console.print(Pretty(data_to_render))
                return

            if not data_to_render:  # Handle empty lists or dicts
                console.print(Pretty(data_to_render))
                return

            formatted_json = json.dumps(data_to_render, indent=2, default=str)
            syntax = Syntax(formatted_json, "json", theme="monokai", line_numbers=True)
            console.print(syntax)
        except (TypeError, OverflowError):
            console.print(Pretty(data_to_render))
