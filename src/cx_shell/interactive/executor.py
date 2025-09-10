import asyncio
from typing import List, Any, Optional
from dataclasses import dataclass
from ast import literal_eval
import structlog

from lark import Lark, Transformer, v_args
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich import box

from ..engine.connector.service import ConnectorService
from ..management.session_manager import SessionManager
from ..management.variable_manager import VariableManager
from ..management.flow_manager import FlowManager
from ..management.query_manager import QueryManager
from ..management.script_manager import ScriptManager
from ..management.connection_manager import ConnectionManager
from ..management.open_manager import OpenManager
from ..management.app_manager import AppManager
from ..management.process_manager import ProcessManager
from .agent_orchestrator import AgentOrchestrator
from ..management.compile_manager import CompileManager
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
    AgentCommand,
    ProcessCommand,
    CompileCommand,
)
from .session import SessionState
from ..data.agent_schemas import DryRunResult
from ..utils import get_pkg_root
from .output_handler import IOutputHandler


console = Console()
logger = structlog.get_logger(__name__)


@dataclass
class VariableLookup:
    """A simple data class to represent looking up a variable in the session."""

    var_name: str


@v_args(inline=True)
class CommandTransformer(Transformer):
    """Transforms the Lark parse tree into our executable Command objects."""

    def command_line(self, executable, formatter=None):
        merged_options = {}
        if formatter:
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

    def builtin_command(self, cmd):
        return cmd

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

    def agent_command(self, goal):
        return AgentCommand(literal_eval(goal.value))

    # Pass-throughs for all command groups
    def session_command(self, cmd_obj):
        return cmd_obj

    def session_subcommand(self, cmd_obj):
        return cmd_obj

    def variable_command(self, cmd_obj):
        return cmd_obj

    def variable_subcommand(self, cmd_obj):
        return cmd_obj

    def flow_command(self, cmd_obj):
        return cmd_obj

    def flow_subcommand(self, cmd_obj):
        return cmd_obj

    def query_command(self, cmd_obj):
        return cmd_obj

    def query_subcommand(self, cmd_obj):
        return cmd_obj

    def script_command(self, cmd_obj):
        return cmd_obj

    def script_subcommand(self, cmd_obj):
        return cmd_obj

    def connection_command(self, cmd_obj):
        return cmd_obj

    def connection_subcommand(self, cmd_obj):
        return cmd_obj

    def app_command(self, cmd_obj):
        return cmd_obj

    def app_subcommand(self, cmd_obj):
        return cmd_obj

    def process_command(self, cmd_obj):
        return cmd_obj

    def process_subcommand(self, cmd_obj):
        return cmd_obj

    # --- Methods for specific subcommands ---
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

    def flow_run(self, name, *kv_pairs):
        return FlowCommand("run", name=name.value, args=dict(kv_pairs))

    def query_list(self):
        return QueryCommand("list")

    def query_run(self, on_alias, name, *kv_pairs):
        return QueryCommand(
            "run",
            name=name.value,
            named_args={"on_alias": on_alias.value, "args": dict(kv_pairs)},
        )

    def script_list(self):
        return ScriptCommand("list")

    def script_run(self, name, *kv_pairs):
        return ScriptCommand("run", name=name.value, args=dict(kv_pairs))

    def connection_list(self):
        return ConnectionCommand("list")

    def connection_create(self, command_args=None):
        args_dict = {key.lstrip("-"): value for key, value in (command_args or [])}
        return ConnectionCommand("create", named_args=args_dict)

    def open_command_with_args(
        self, asset_type_token, asset_name_token=None, command_args=None
    ):
        asset_type = asset_type_token.value
        asset_name = asset_name_token.value if asset_name_token else None
        args_dict = {key.lstrip("-"): value for key, value in (command_args or [])}
        return OpenCommand(asset_type, asset_name, args_dict)

    def app_list(self):
        return AppCommand("list", args={})

    def app_install(self, command_args):
        args_dict = dict(command_args)
        source_keys = {"--id", "--path", "--url"}
        provided_keys = set(args_dict.keys())
        if len(provided_keys.intersection(source_keys)) != 1:
            raise ValueError(
                "`app install` requires exactly one of --id, --path, or --url."
            )
        # --- THIS IS THE FIX ---
        # Changed 'k' to 'key' to match the loop variable.
        cleaned_args = {key.lstrip("-"): v for key, v in args_dict.items()}
        # --- END FIX ---
        return AppCommand("install", args=cleaned_args)

    def app_uninstall(self, arg):
        return AppCommand("uninstall", args={"id": arg.value})

    def app_sync(self):
        return AppCommand("sync", args={})

    def app_package(self, arg):
        return AppCommand("package", args={"path": arg.value})

    def app_search(self, query=None):
        query_val = query.value if query else None
        return AppCommand("search", args={"query": query_val})

    def compile_command_with_args(self, command_args):
        args_dict = {
            key.lstrip("-").replace("-", "_"): value for key, value in command_args
        }
        return CompileCommand(named_args=args_dict)

    def process_list(self):
        return ProcessCommand("list")

    def process_logs(self, arg, follow=None):
        return ProcessCommand("logs", arg.value, follow is not None)

    def process_stop(self, arg):
        return ProcessCommand("stop", arg.value)

    # --- Argument & Terminal Processing ---
    def command_args(self, *named_args):
        return list(named_args)

    def arguments(self, *args):
        return dict(args)

    def kv_pair(self, key, value):
        return key.value, value

    def kw_argument(self, key, value):
        return key.value, value

    def named_argument(self, flag, value):
        processed_value = value
        if hasattr(value, "type"):
            if value.type == "STRING":
                processed_value = literal_eval(value.value)
            else:
                processed_value = value.value
        try:
            processed_value = int(processed_value)
        except (ValueError, TypeError):
            try:
                processed_value = float(processed_value)
            except (ValueError, TypeError):
                pass
        return (flag.value, processed_value)

    def value(self, v):
        if hasattr(v, "type"):
            if v.type == "JINJA_BLOCK":
                return v.value
            if v.type in ("STRING", "NUMBER"):
                return literal_eval(v.value)
            if v.type == "ARG":
                return v.value
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

    def __init__(self, state: SessionState, output_handler: IOutputHandler):
        """
        Initializes the executor with a session state and a dedicated output handler.
        """
        self.state = state
        self.output_handler = output_handler  # Store the handler
        self.service = ConnectorService()
        self.session_manager = SessionManager()
        self.variable_manager = VariableManager()
        self.flow_manager = FlowManager()
        self.query_manager = QueryManager()
        self.script_manager = ScriptManager()
        self.connection_manager = ConnectionManager()
        self.open_manager = OpenManager()
        self.app_manager = AppManager()
        self.process_manager = ProcessManager()
        self.compile_manager = CompileManager()
        self.builtin_commands = {
            "connect": self.execute_connect,
            "connections": self.execute_list_connections,
            "help": self.execute_help,
        }
        self._orchestrator: Optional[AgentOrchestrator] = None
        pkg_root = get_pkg_root()
        grammar_path = pkg_root / "interactive" / "grammar" / "cx.lark"
        with open(grammar_path, "r", encoding="utf-8") as f:
            self.parser = Lark(f.read(), start="start", parser="lalr")
        self.transformer = CommandTransformer()

    @property
    def orchestrator(self) -> AgentOrchestrator:
        """Lazily initializes the AgentOrchestrator on first use."""
        if self._orchestrator is None:
            logger.debug("executor.lazy_load", component="AgentOrchestrator")
            self._orchestrator = AgentOrchestrator(self.state, self)
        return self._orchestrator

    async def execute(
        self, command_text: str, piped_input: Any = None
    ) -> Optional[SessionState]:
        """
        The main entry point. Parses, executes, and delegates the result to the output handler.
        """
        if not command_text.strip():
            return None

        executable_obj = None
        formatter_options = None

        try:
            parsed_tree = self.parser.parse(command_text)
            executable_obj, formatter_options = self.transformer.transform(parsed_tree)

            result = await self._execute_executable(
                executable_obj, piped_input=piped_input, is_agent_execution=False
            )

            if isinstance(result, SessionState):
                return result

            # The handler is now optional. If it exists, use it.
            if self.output_handler:
                await self.output_handler.handle_result(
                    result, executable_obj, formatter_options
                )
            else:
                # This case is for when the server calls execute directly,
                # but the result should be handled by the WebSocket handler,
                # which has already happened. So we just return the raw data.
                return result

        except Exception as e:
            original_exc = getattr(e, "orig_exc", e)
            error_result = {"error": f"{type(original_exc).__name__}: {original_exc}"}
            if self.output_handler:
                await self.output_handler.handle_result(
                    error_result, executable_obj, formatter_options
                )
            else:
                return error_result

        return None

    async def _execute_executable(
        self, executable: Any, piped_input: Any = None, is_agent_execution: bool = False
    ) -> Any:
        """
        Recursively executes any command object, now correctly handling arguments
        that might contain formatter flags.
        """
        if isinstance(executable, PipelineCommand):
            current_input = piped_input
            for item in executable.commands:
                current_input = await self._execute_executable(
                    item,
                    piped_input=current_input,
                    is_agent_execution=is_agent_execution,
                )
                if isinstance(current_input, dict) and "error" in current_input:
                    break
            return current_input

        if isinstance(executable, AssignmentCommand):
            result = await self._execute_executable(
                executable.command_to_run,
                piped_input,
                is_agent_execution=is_agent_execution,
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

        if isinstance(executable, Command):
            if getattr(executable, "subcommand", None) == "run" or isinstance(
                executable, (DotNotationCommand, PositionalArgActionCommand)
            ):
                with console.status("Executing command...", spinner="dots") as status:
                    if isinstance(
                        executable, (FlowCommand, QueryCommand, ScriptCommand)
                    ):
                        # For these commands, the universal formatters are parsed as part of their
                        # arguments dictionary. We do NOT need to clean them here, as the top-level
                        # `execute` method already separates them. This logic can be simplified.
                        if isinstance(executable, FlowCommand):
                            status.update(f"Running flow '{executable.name}'...")
                            return await self.flow_manager.run_flow(
                                self.state,
                                self.service,
                                executable.name,
                                executable.args,
                            )
                        if isinstance(executable, QueryCommand):
                            on_alias = executable.named_args.get("on_alias")
                            query_args = executable.named_args.get("args", {})
                            status.update(
                                f"Running query '{executable.name}' on '{on_alias}'..."
                            )
                            return await self.query_manager.run_query(
                                self.state,
                                self.service,
                                executable.name,
                                on_alias,
                                query_args,
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
            else:
                return await self._dispatch_management_command(
                    executable, is_agent_execution
                )

        raise TypeError(f"Cannot execute object of type: {type(executable).__name__}")

    async def _dispatch_management_command(
        self, command: Command, is_agent_execution: bool
    ) -> Any:
        """
        Routes management commands. Data-producing commands (like 'list') return
        structured data for the handler to process. Action-performing commands
        manage their own interactive output or return simple confirmation messages.
        """

        # --- SECTION 1: DATA-PRODUCING MANAGEMENT COMMANDS ---
        # These commands return structured data (e.g., a list of dicts) that the
        # IOutputHandler will be responsible for rendering.

        if hasattr(command, "subcommand") and command.subcommand == "list":
            if isinstance(command, ConnectionCommand):
                return self.connection_manager.list_connections()
            if isinstance(command, FlowCommand):
                return self.flow_manager.list_flows()
            if isinstance(command, QueryCommand):
                return self.query_manager.list_queries()
            if isinstance(command, ScriptCommand):
                return self.script_manager.list_scripts()
            if isinstance(command, SessionCommand):
                return self.session_manager.list_sessions()
            if isinstance(command, VariableCommand):
                return self.variable_manager.list_variables(self.state)
            if isinstance(command, AppCommand):
                return await self.app_manager.list_installed_apps()
            if isinstance(command, ProcessCommand):
                return self.process_manager.list_processes()

        if isinstance(command, AppCommand) and command.subcommand == "search":
            return await self.app_manager.search(command.args.get("query"))

        if isinstance(command, InspectCommand):
            return await command.execute(self.state, self.service, None)

        # Built-in 'connections' command is a special case of a data-producing command.
        if isinstance(command, BuiltinCommand) and command.command == "connections":
            connections_list = [
                {"Alias": alias, "Source": source}
                for alias, source in self.state.connections.items()
            ]
            return connections_list

        # --- SECTION 2: ACTION-PERFORMING MANAGEMENT COMMANDS ---
        # These commands perform an action, manage their own interactive output,
        # or return a simple string confirmation.

        command_prints_own_output = False
        simple_confirmation_message = None

        if isinstance(command, BuiltinCommand):
            # This handles 'connect' and 'help'
            command_prints_own_output = True
            handler = self.builtin_commands.get(command.command)
            if handler:
                await handler(command.args) if asyncio.iscoroutinefunction(
                    handler
                ) else handler(command.args)

        elif isinstance(command, ConnectionCommand) and command.subcommand == "create":
            await self.connection_manager.create_interactive(
                command.named_args.get("blueprint")
            )
            command_prints_own_output = True

        elif isinstance(command, AppCommand):
            command_prints_own_output = True
            if command.subcommand == "install":
                await self.app_manager.install(command.args)
            elif command.subcommand == "uninstall":
                await self.app_manager.uninstall(command.args["id"])
            elif command.subcommand == "package":
                await self.app_manager.package(command.args["path"])

        elif isinstance(command, SessionCommand):
            if command.subcommand == "status":
                self.session_manager.show_status(self.state)
                command_prints_own_output = True
            elif command.subcommand == "save":
                simple_confirmation_message = self.session_manager.save_session(
                    self.state, command.arg
                )
            elif command.subcommand == "rm":
                simple_confirmation_message = await self.session_manager.delete_session(
                    command.arg
                )
            elif command.subcommand == "load":
                return self.session_manager.load_session(command.arg)

        elif isinstance(command, VariableCommand) and command.subcommand == "rm":
            simple_confirmation_message = self.variable_manager.delete_variable(
                self.state, command.arg
            )

        elif isinstance(command, OpenCommand):
            command_prints_own_output = True
            handler = command.named_args.get("in", "default")
            on_alias = command.named_args.get("on")
            await self.open_manager.open_asset(
                self.state,
                self.service,
                command.asset_type,
                command.asset_name,
                handler,
                on_alias,
            )

        elif isinstance(command, ProcessCommand) and command.subcommand == "logs":
            command_prints_own_output = True
            self.process_manager.get_logs(command.arg, command.follow)

        elif isinstance(command, CompileCommand):
            command_prints_own_output = True
            await self.compile_manager.run_compile(**command.named_args)

        elif isinstance(command, AgentCommand):
            command_prints_own_output = True
            await self.orchestrator.start_session(command.goal)

        # --- Context-Aware Return Logic for Action-Performing Commands ---
        if is_agent_execution:
            return {
                "status": "success",
                "message": f"Management command '{type(command).__name__}' executed successfully.",
            }
        else:
            if simple_confirmation_message:
                return simple_confirmation_message
            if command_prints_own_output:
                return None
            # Fallback for any unhandled case
            return {"status": "success", "message": "Command executed."}

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
        return {
            "status": "success",
            "message": f"Builtin command '{command.command}' executed.",
        }

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

    async def dry_run(self, command_text: str) -> DryRunResult:
        """Parses a command and routes it to the appropriate dry_run method."""
        try:
            executable_obj, _ = self.transformer.transform(
                self.parser.parse(command_text)
            )

            # Dispatcher for dry_run
            if isinstance(executable_obj, DotNotationCommand):
                step = executable_obj.to_step(self.state)
                if step.run.action == "run_declarative_action":
                    conn, secrets = await self.service.resolver.resolve(
                        step.connection_source
                    )
                    strategy = self.service._get_strategy_for_connection_model(conn)

                    if hasattr(strategy, "dry_run"):
                        return await strategy.dry_run(
                            conn, secrets, step.run.model_dump()
                        )

            # TODO: Implement dry_run dispatch for SQL, FS, and other strategies.

            # Default fallback if no specific dry_run is implemented.
            return DryRunResult(
                indicates_failure=False, message="Command is syntactically valid."
            )

        except Exception as e:
            return DryRunResult(
                indicates_failure=True, message=f"Command is invalid. Error: {e}"
            )
