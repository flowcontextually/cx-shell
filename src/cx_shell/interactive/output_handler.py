# /home/dpwanjala/repositories/cx-shell/src/cx_shell/interactive/output_handler.py

import json
from abc import ABC, abstractmethod
from typing import Any, Optional, Dict

from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.pretty import Pretty
from rich.syntax import Syntax
from rich.table import Table

# Import the command classes to identify them
from .commands import (
    Command,
    InspectCommand,
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
        self, result: Any, executable: Optional[Command], options: Optional[Dict] = None
    ):
        """
        The definitive, corrected version of the result handler.
        """
        # --- THIS IS THE FINAL, CORRECTED LOGIC ---

        if result is None:
            # We explicitly print None so the user knows the result was None, not that the shell broke.
            console.print(Pretty(None))
            return

        options = options or {}

        if isinstance(result, str):
            console.print(f"[bold green]✓[/bold green] {result}")
            return

        if isinstance(result, dict) and "error" in result:
            console.print(f"[bold red]Runtime Error:[/bold red] {result['error']}")
            return

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

        # --- Final Presentation Logic ---
        data_to_render = result  # The result is now guaranteed to be clean
        output_mode = options.get("output_mode", "default")
        is_list_of_dicts = (
            isinstance(data_to_render, list)
            and bool(data_to_render)
            and all(isinstance(i, dict) for i in data_to_render)
        )

        # The core rendering decision.
        # If the user asks for a table, or if the result is a list of objects and no other mode is specified, render a table.
        if output_mode == "table" or (is_list_of_dicts and output_mode == "default"):
            if is_list_of_dicts:
                title = "Data View"
                # ... (dynamic title logic can go here) ...
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
                    pass  # Fallback to JSON

        # Fallback for all other cases is to print as JSON or a Pretty representation.
        try:
            if not isinstance(data_to_render, (dict, list)) or not data_to_render:
                console.print(Pretty(data_to_render))
                return

            formatted_json = json.dumps(data_to_render, indent=2, default=str)
            syntax = Syntax(formatted_json, "json", theme="monokai", line_numbers=True)
            console.print(syntax)
        except (TypeError, OverflowError):
            console.print(Pretty(data_to_render))
