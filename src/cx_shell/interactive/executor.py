import asyncio
import json
from pathlib import Path
from typing import List, Any
from dataclasses import dataclass
from ast import literal_eval
import jmespath

from lark import Lark, Transformer, v_args
from lark.exceptions import LarkError
from rich.console import Console
from rich.panel import Panel
from rich.syntax import Syntax
from rich.table import Table
from rich import box
from rich.pretty import Pretty

from ..engine.connector.service import ConnectorService
from ..management.session_manager import SessionManager
from ..management.variable_manager import VariableManager
from ..management.flow_manager import FlowManager
from ..management.query_manager import QueryManager
from ..management.script_manager import ScriptManager
from ..management.connection_manager import ConnectionManager
from ..management.open_manager import OpenManager
from ..management.app_manager import AppManager
from .commands import (
    Command,
    DotNotationCommand,
    BuiltinCommand,
    PositionalArgActionCommand,
    AssignmentCommand,
    InspectCommand,
    PipelineCommand,
    SessionCommand,
    VariableCommand,
    FlowCommand,
    QueryCommand,
    ScriptCommand,
    ConnectionCommand,
    OpenCommand,
    AppCommand,
)
from .session import SessionState

console = Console()


@dataclass
class VariableLookup:
    """A simple data class to represent looking up a variable in the session."""

    var_name: str


@v_args(inline=True)
class CommandTransformer(Transformer):
    """Transforms the Lark parse tree into our executable objects and formatter options."""

    # This class is an internal implementation detail and does not require extensive docstrings.
    # Its correctness is verified by the successful parsing of commands.
    def command_line(self, executable, formatter=None):
        """
        Handles the top-level rule. Receives the executed command/pipeline
        and EITHER a list of formatter options OR None.
        """
        merged_options = {}
        if formatter:  # formatter is a list of dicts from the formatter_option+ rule
            for option_dict in formatter:
                merged_options.update(option_dict)

        return executable, merged_options or None

    def executable(self, exec_obj):
        return exec_obj

    def single_executable(self, exec_obj):
        return exec_obj

    def pipeline(self, *items):
        return PipelineCommand(list(items))

    def assignment(self, var_name, executable):
        return AssignmentCommand(var_name.value, executable)

    def single_command(self, command):
        return command

    def variable_lookup(self, var_name):
        return VariableLookup(var_name.value)

    def formatter(self, *options):
        return list(options)

    def formatter_option(self, option):
        return option

    def output_option(self, mode):
        return {"output_mode": mode.value}

    def columns_option(self, columns):
        return {"columns": columns}

    def query_option(self, query_str):
        return {"query": literal_eval(query_str.value)}

    def column_list(self, *cols):
        return [c.value for c in cols]

    def dot_notation_kw_action(self, alias, action_name, arguments=None):
        return DotNotationCommand(alias.value, action_name.value, arguments or {})

    def dot_notation_pos_action(self, alias, action_name, string_arg):
        return PositionalArgActionCommand(
            alias.value, action_name.value, literal_eval(string_arg.value)
        )

    def connect_command(self, source, alias):
        return BuiltinCommand(["connect", source.value, "--as", alias.value])

    def connections_command(self):
        return BuiltinCommand(["connections"])

    def help_command(self):
        return BuiltinCommand(["help"])

    def inspect_command(self, var_name):
        return InspectCommand(var_name.value)

    def session_list(self):
        return SessionCommand("list")

    def session_save(self, name):
        return SessionCommand("save", name.value)

    def session_load(self, name):
        return SessionCommand("load", name.value)

    def session_rm(self, name):
        return SessionCommand("rm", name.value)

    def session_status(self):
        return SessionCommand("status")

    def variable_list(self):
        return VariableCommand("list")

    def variable_rm(self, var_name):
        return VariableCommand("rm", var_name.value)

    def flow_list(self):
        return FlowCommand("list")

    # def flow_run(self, flow_name, arguments=None):
    #     return FlowCommand("run", name=flow_name.value, args=arguments)

    def flow_run_with_args(self, flow_name, arguments=None):
        return FlowCommand("run", name=flow_name.value, args=arguments or {})

    def flow_run_no_args(self, flow_name):
        return FlowCommand("run", name=flow_name.value, args={})

    def query_list(self):
        return QueryCommand("list")

    def query_run(self, on_alias, query_name, arguments=None):
        return QueryCommand(
            "run", name=query_name.value, on_alias=on_alias.value, args=arguments
        )

    def script_list(self):
        return ScriptCommand("list")

    def script_run(self, script_name, arguments=None):
        return ScriptCommand("run", name=script_name.value, args=arguments)

    def connection_list(self):
        return ConnectionCommand("list")

    def connection_create(self, blueprint_str):
        return ConnectionCommand("create", blueprint=literal_eval(blueprint_str.value))

    def open_command(self, *args):
        asset_type = args[0].value
        asset_name, handler, on_alias = None, None, None
        remaining_args = list(args[1:])
        if remaining_args and not asset_type.startswith("{{"):
            if not (hasattr(remaining_args[0], "data")):
                asset_name = remaining_args.pop(0).value
        for arg in remaining_args:
            if hasattr(arg, "data"):
                if arg.data == "on_alias":
                    on_alias = arg.children[0].value
                elif arg.data == "handler":
                    handler = arg.children[0].value
        return OpenCommand(asset_type, asset_name, handler, on_alias)

    def app_list(self):
        return AppCommand("list")

    def app_install(self, arg):
        return AppCommand("install", arg.value)

    def app_uninstall(self, arg):
        return AppCommand("uninstall", arg.value)

    def app_sync(self):
        return AppCommand("sync")

    def arguments(self, *args):
        return dict(args)

    def kw_argument(self, key, value):
        return key.value, value

    def value(self, v):
        if hasattr(v, "type"):
            if v.type == "JINJA_BLOCK":
                return v.value
            if v.type in ("STRING", "NUMBER"):
                return literal_eval(v.value)
            if v.type == "CNAME":
                return v.value
        return v

    def true(self, _):
        return True

    def false(self, _):
        return False

    def null(self, _):
        return None


class CommandExecutor:
    """Orchestrates the parsing, execution, and presentation of all shell commands."""

    def __init__(self, state: SessionState):
        """Initializes the executor and all its subordinate managers."""
        self.state = state
        self.service = ConnectorService()
        self.session_manager = SessionManager()
        self.variable_manager = VariableManager()
        self.flow_manager = FlowManager()
        self.query_manager = QueryManager()
        self.script_manager = ScriptManager()
        self.connection_manager = ConnectionManager()
        self.open_manager = OpenManager()
        self.app_manager = AppManager()
        self.builtin_commands = {
            "connect": self.execute_connect,
            "connections": self.execute_list_connections,
            "help": self.execute_help,
        }
        grammar_path = Path(__file__).parent / "grammar" / "cx.lark"
        with open(grammar_path, "r", encoding="utf-8") as f:
            self.parser = Lark(f.read(), start="start", parser="lalr")
        self.transformer = CommandTransformer()

    async def execute(self, command_text: str) -> SessionState | None:
        """
        The main entry point for the executor.

        Parses a raw command string, executes the resulting command object,
        and prints the final result. Returns a new SessionState object if a
        session was loaded, otherwise returns None.
        """
        if not command_text.strip():
            return None
        try:
            executable_obj, formatter_options = self.transformer.transform(
                self.parser.parse(command_text)
            )
            result = await self._execute_executable(executable_obj)
            if isinstance(result, SessionState):
                return result
            self._print_result(result, executable_obj, formatter_options)
        except LarkError as e:
            context = e.get_context(command_text, span=40)
            console.print(
                f"[bold red]Syntax Error:[/bold red] Invalid syntax near column {e.column}.\n{context}"
            )
        except Exception as e:
            original_exc = getattr(e, "orig_exc", e)
            console.print(
                f"[bold red]{type(original_exc).__name__}:[/bold red] {original_exc}"
            )
        return None

    async def _execute_executable(
        self, executable: Any, piped_input: Any = None
    ) -> Any:
        """
        Recursively executes any executable object (pipeline, assignment, command, etc.)
        and returns the final data result. This is the core orchestration method.
        """
        # --- 1. Handle Structural/Recursive Executables ---
        if isinstance(executable, PipelineCommand):
            current_input = piped_input
            for item in executable.commands:
                current_input = await self._execute_executable(
                    item, piped_input=current_input
                )
                if isinstance(current_input, dict) and "error" in current_input:
                    break
            return current_input

        if isinstance(executable, AssignmentCommand):
            result = await self._execute_executable(
                executable.command_to_run, piped_input
            )
            if not (isinstance(result, dict) and "error" in result):
                self.state.variables[executable.var_name] = result
            return (
                result
                if isinstance(result, dict) and "error" in result
                else f"Variable '{executable.var_name}' set."
            )

        if isinstance(executable, VariableLookup):
            if executable.var_name not in self.state.variables:
                raise ValueError(f"Variable '{executable.var_name}' not found.")
            if piped_input is not None:
                raise ValueError("Cannot pipe data into a variable lookup.")
            return self.state.variables[executable.var_name]

        # --- 2. Dispatch All Command Types ---
        if isinstance(executable, Command):
            # If it's a 'run' subcommand or a dot-notation command, it's a "heavy" data-producing operation.
            if getattr(executable, "subcommand", None) == "run" or isinstance(
                executable, (DotNotationCommand, PositionalArgActionCommand)
            ):
                with console.status("Executing command...", spinner="dots") as status:
                    if isinstance(executable, FlowCommand):
                        status.update(f"Running flow '{executable.name}'...")
                        return await self.flow_manager.run_flow(
                            self.state, self.service, executable.name, executable.args
                        )
                    if isinstance(executable, QueryCommand):
                        status.update(
                            f"Running query '{executable.name}' on '{executable.on_alias}'..."
                        )
                        return await self.query_manager.run_query(
                            self.state,
                            self.service,
                            executable.name,
                            executable.on_alias,
                            executable.args,
                        )
                    if isinstance(executable, ScriptCommand):
                        status.update(f"Running script '{executable.name}'...")
                        return await self.script_manager.run_script(
                            self.state,
                            self.service,
                            executable.name,
                            executable.args,
                            piped_input,
                        )
                    if isinstance(
                        executable, (DotNotationCommand, PositionalArgActionCommand)
                    ):
                        return await executable.execute(
                            self.state, self.service, status, piped_input=piped_input
                        )

            # Otherwise, it's a "lightweight" management command.
            else:
                return await self._dispatch_management_command(executable)

        # This line should now be unreachable if the grammar is correct, but serves as a safety net.
        raise TypeError(f"Cannot execute object of type: {type(executable).__name__}")

    async def _dispatch_management_command(self, command: Command) -> Any:
        """Routes all non-data-producing management commands to their respective handlers."""
        if isinstance(command, BuiltinCommand):
            handler = self.builtin_commands.get(command.command)
            if handler:
                return (
                    await handler(command.args)
                    if asyncio.iscoroutinefunction(handler)
                    else handler(command.args)
                )
        elif isinstance(command, ConnectionCommand):
            if command.subcommand == "list":
                self.connection_manager.list_connections()
            elif command.subcommand == "create":
                await self.connection_manager.create_interactive(command.blueprint)
        elif isinstance(command, SessionCommand):
            if command.subcommand == "list":
                self.session_manager.list_sessions()
            elif command.subcommand == "status":
                self.session_manager.show_status(self.state)
            elif command.subcommand == "save":
                return self.session_manager.save_session(self.state, command.arg)
            elif command.subcommand == "rm":
                return await self.session_manager.delete_session(command.arg)
            elif command.subcommand == "load":
                return self.session_manager.load_session(command.arg)
        elif isinstance(command, VariableCommand):
            if command.subcommand == "list":
                self.variable_manager.list_variables(self.state)
            elif command.subcommand == "rm":
                return self.variable_manager.delete_variable(self.state, command.arg)
        elif isinstance(command, FlowCommand) and command.subcommand == "list":
            self.flow_manager.list_flows()
        elif isinstance(command, QueryCommand) and command.subcommand == "list":
            self.query_manager.list_queries()
        elif isinstance(command, ScriptCommand) and command.subcommand == "list":
            self.script_manager.list_scripts()
        elif isinstance(command, OpenCommand):
            await self.open_manager.open_asset(
                self.state,
                self.service,
                command.asset_type,
                command.asset_name,
                command.handler,
                command.on_alias,
            )
        elif isinstance(command, AppCommand):
            if command.subcommand == "list":
                await self.app_manager.list_installed_apps()
            elif command.subcommand == "install":
                with console.status(f"Installing application '{command.arg}'..."):
                    await self.app_manager.install(command.arg)
            elif command.subcommand == "uninstall":
                await self.app_manager.uninstall(command.arg)
            else:
                # For now, placeholder for sync
                console.print(
                    f"[yellow]'{command.subcommand}' command is not yet fully implemented.[/yellow]"
                )
            return  # App commands don't return data to the pipeline
        elif isinstance(command, InspectCommand):
            return await command.execute(self.state, self.service, None)
        else:
            console.print(
                f"[bold red]Error:[/bold red] Unknown management command '{command}'."
            )
        return None

    def _print_result(self, result: Any, executable: Any, options: dict | None = None):
        """Handles the final rendering of a command's result to the console."""
        if result is None:
            return
        options = options or {}

        # 1. Handle simple string confirmation messages from management commands.
        if isinstance(result, str):
            # This catches "Variable 'x' set", "Session 'y' saved", etc.
            console.print(f"[bold green]✓[/bold green] {result}")
            return

        # 2. Handle the special panel format for the 'inspect' command.
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
            if "item_zero_preview" in summary:
                panel_content += (
                    f"[bold]Item[0] Preview:[/bold] {summary['item_zero_preview']}"
                )
            if "value_preview" in summary:
                panel_content += (
                    f"[bold]Value Preview:[/bold] {summary['value_preview']}"
                )
            console.print(
                Panel(panel_content, title="Object Inspector", border_style="yellow")
            )
            return

        # 3. Handle generic runtime errors that may have been passed through.
        if isinstance(result, dict) and "error" in result:
            console.print(f"[bold red]Runtime Error:[/bold red] {result['error']}")
            return

        # 4. Intelligently unpack the raw result to get to the core data payload.
        data_to_process = result
        if isinstance(data_to_process, dict) and "results" in data_to_process:
            data_to_process = data_to_process["results"]
        if isinstance(data_to_process, dict) and "data" in data_to_process:
            data_to_process = data_to_process["data"]
        if isinstance(data_to_process, dict) and "content" in data_to_process:
            try:
                data_to_process = json.loads(data_to_process["content"])
            except (json.JSONDecodeError, TypeError):
                pass

        # 5. Apply the JMESPath query to the unpacked data.
        data_to_render = data_to_process
        if options.get("query"):
            try:
                data_to_render = jmespath.search(options["query"], data_to_process)
            except Exception as e:
                console.print(f"[bold red]JMESPath Error:[/bold red] {e}")
                return

        # 6. Render the final, processed data based on the output mode.
        output_mode = options.get("output_mode", "default")

        if (
            output_mode == "table"
            and isinstance(data_to_render, list)
            and data_to_render
        ):
            # Check for list of dicts or list of lists
            is_list_of_dicts = all(isinstance(i, dict) for i in data_to_render)
            is_list_of_lists = all(isinstance(i, list) for i in data_to_render)

            if is_list_of_dicts or is_list_of_lists:
                table = Table(title="Data View", box=box.ROUNDED)
                headers = options.get("columns")

                if is_list_of_dicts:
                    if headers is None:
                        headers = list(data_to_render[0].keys())
                    for header in headers:
                        table.add_column(str(header), style="cyan", overflow="fold")
                    for row in data_to_render:
                        table.add_row(*(str(row.get(h, "")) for h in headers))

                elif is_list_of_lists:
                    if headers is None:
                        console.print(
                            "[bold red]Error:[/bold red] To display this data as a table, you must provide column names using the `--columns` flag."
                        )
                        return
                    if len(headers) != len(data_to_render[0]):
                        console.print(
                            f"[bold red]Error:[/bold red] The number of columns provided ({len(headers)}) does not match the data structure ({len(data_to_render[0])})."
                        )
                        return
                    for header in headers:
                        table.add_column(str(header), style="cyan", overflow="fold")
                    for row in data_to_render:
                        table.add_row(*(str(item) for item in row))

                console.print(table)
                return

        try:
            formatted_json = json.dumps(data_to_render, indent=2, default=str)
            syntax = Syntax(formatted_json, "json", theme="monokai", line_numbers=True)
            console.print(syntax)
        except (TypeError, OverflowError):
            console.print(Pretty(data_to_render))

    async def _dispatch_builtin(self, command: Command) -> Any:
        """Routes built-in and management commands to the correct handler."""

        # --- NEW: Handle Session Commands ---
        if isinstance(command, SessionCommand):
            if command.subcommand == "list":
                self.session_manager.list_sessions()
            elif command.subcommand == "save":
                if not command.arg:
                    raise ValueError("Usage: session save <session-name>")
                self.session_manager.save_session(self.state, command.arg)
            elif command.subcommand == "rm":
                if not command.arg:
                    raise ValueError("Usage: session rm <session-name>")
                await self.session_manager.delete_session(command.arg)
            elif command.subcommand == "status":
                self.session_manager.show_status(self.state)
            elif command.subcommand == "load":
                if not command.arg:
                    raise ValueError("Usage: session load <session-name>")
                return self.session_manager.load_session(command.arg)
            return None  # Most session commands don't return a value

        if isinstance(command, VariableCommand):
            if command.subcommand == "list":
                self.variable_manager.list_variables(self.state)
            elif command.subcommand == "rm":
                if not command.arg:
                    raise ValueError("Usage: var rm <variable-name>")
                self.variable_manager.delete_variable(self.state, command.arg)
            return None  # Variable commands don't return values for the pipeline

        if isinstance(command, InspectCommand):
            return await command.execute(self.state, self.service, None)

        if isinstance(command, BuiltinCommand):
            handler = self.builtin_commands.get(command.command)
            if handler:
                await handler(command.args) if asyncio.iscoroutinefunction(
                    handler
                ) else handler(command.args)
            else:
                console.print(
                    f"[bold red]Error:[/bold red] Unknown command '{command.command}'."
                )

        return None

    def execute_help(self, args: List[str]):
        """Displays a structured and comprehensive guide for the interactive shell."""
        console.print()
        title = Panel(
            "[bold yellow]Welcome to the Contextual Shell (`cx`) v0.2.0[/bold yellow]",
            expand=False,
            border_style="yellow",
        )
        console.print(title)

        console.print(
            "\n`cx` is an interactive shell for managing workspace assets and running data workflows."
        )

        # --- Core Commands Table ---
        builtins_table = Table(
            title="[bold cyan]Core Commands[/bold cyan]",
            box=box.MINIMAL,
            padding=(0, 1),
        )
        builtins_table.add_column("Command", style="yellow", no_wrap=True)
        builtins_table.add_column("Description")
        builtins_table.add_row(
            "connect <source> --as <alias>",
            "Activate a connection for the current session (e.g., `connect user:github --as gh`).",
        )
        builtins_table.add_row(
            "connections", "List all active connections in the current session."
        )
        builtins_table.add_row("exit | quit", "Exit the interactive shell.")
        builtins_table.add_row("help", "Show this help message.")
        console.print(builtins_table)

        # --- Asset Management Table ---
        assets_table = Table(
            title="[bold cyan]Workspace Asset Management[/bold cyan]",
            box=box.MINIMAL,
            padding=(0, 1),
        )
        assets_table.add_column("Command", style="yellow", no_wrap=True)
        assets_table.add_column("Description")
        assets_table.add_row(
            "session [list|save|load|rm|status]",
            "Manage persistent workspace sessions.",
        )
        assets_table.add_row("var [list|rm]", "Manage in-memory session variables.")
        assets_table.add_row(
            "flow [list|run]", "Manage and run reusable `.flow.yaml` workflows."
        )
        assets_table.add_row(
            "query [list|run]", "Manage and run reusable `.sql` queries."
        )
        assets_table.add_row(
            "script [list|run]", "Manage and run reusable `.py` scripts."
        )
        assets_table.add_row(
            "connection [list|create]",
            "Manage the connection configuration files on disk.",
        )
        assets_table.add_row(
            "open <type> [name] [--in <handler>]",
            "Open assets in their default or a specified application (e.g., `open flow my-flow --in vscode`).",
        )
        assets_table.add_row(
            "inspect <variable>", "Display a detailed summary of a session variable."
        )
        console.print(assets_table)

        # --- Execution & Formatting ---
        execution_table = Table(
            title="[bold cyan]Execution & Formatting[/bold cyan]",
            box=box.MINIMAL,
            padding=(0, 1),
        )
        execution_table.add_column("Syntax", style="yellow", no_wrap=True)
        execution_table.add_column("Description")
        execution_table.add_row(
            "<alias>.<action>(...)",
            "Execute a blueprint-defined action on a connection.",
        )
        execution_table.add_row(
            "<command> | <command>",
            "Pipe the output of one command to the input of the next.",
        )
        execution_table.add_row(
            "<variable> = <command>",
            "Assign the result of a command to a session variable.",
        )
        execution_table.add_row(
            "... --output table", "Render the final output as a formatted table."
        )
        execution_table.add_row(
            "... --columns <col1,col2>", "Select specific columns for table output."
        )
        execution_table.add_row(
            "... --query <jmespath>",
            "Filter or reshape the final output using a JMESPath query.",
        )
        console.print(execution_table)
        console.print()

    def execute_list_connections(self, args: List[str]):
        """Lists the currently active connections in the session."""
        if not self.state.connections:
            console.print("No active connections in this session.")
            return

        # --- THIS IS THE FIX ---
        # Replicate the richer table style from the `cx connection list` command.
        table = Table(title="[bold green]Active Session Connections[/bold green]")
        table.add_column("Alias", style="cyan", no_wrap=True)
        table.add_column("Source", style="magenta")
        # --- END FIX ---

        for alias, source in self.state.connections.items():
            table.add_row(str(alias), str(source))

        console.print(table)

    async def execute_connect(self, args: List[str]):
        if len(args) < 3 or args[1].lower() != "--as":
            console.print(
                "[bold red]Invalid syntax.[/bold red] Use: `connect <connection_source> --as <alias>`"
            )
            return
        source, alias = args[0], args[2]
        with console.status(
            f"Attempting to connect to '[yellow]{source}[/yellow]'...", spinner="dots"
        ):
            result = await self.service.test_connection(source)
        if result.get("status") == "success":
            self.state.connections[alias] = source
            console.print(
                f"[bold green]✅ Connection successful.[/bold green] Alias '[cyan]{alias}[/cyan]' is now active."
            )
        else:
            error_message = result.get("message", "An unknown error occurred.")
            console.print(f"[bold red]❌ Connection failed:[/bold red] {error_message}")
