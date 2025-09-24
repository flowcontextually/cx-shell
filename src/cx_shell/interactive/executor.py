# [REPLACE] /home/dpwanjala/repositories/cx-shell/src/cx_shell/interactive/executor.py

import asyncio
import json
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from ast import literal_eval
import jmespath
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
from ..management.workspace_manager import WorkspaceManager
from ..management.index_manager import IndexManager
from ..management.find_manager import FindManager

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
    WorkspaceCommand,
    FindCommand,
)
from .session import SessionState
from ..data.agent_schemas import DryRunResult
from ..utils import get_pkg_root
from .output_handler import IOutputHandler

console = Console()
logger = structlog.get_logger(__name__)


@dataclass
class VariableLookup:
    var_name: str


@v_args(inline=True)
class CommandTransformer(Transformer):
    """Transforms the Lark parse tree into our executable Command objects."""

    def expression(self, pipeline):
        return pipeline

    def pipeline(self, *items):
        clean_commands = [item for item in items if isinstance(item, tuple)]
        return PipelineCommand(clean_commands)

    def command_unit(self, executable, formatter=None):
        return (executable, dict(formatter or []))

    def executable(self, exec_obj):
        return exec_obj

    def single_executable(self, exec_obj):
        return exec_obj

    def assignment(self, var_name, expression):
        return AssignmentCommand(var_name.value, expression)

    def single_command(self, command):
        return command

    def builtin_command(self, cmd):
        return cmd

    def variable_lookup(self, var_name):
        return VariableLookup(var_name.value)

    def formatter(self, *options):
        return options

    def formatter_option(self, option):
        return option

    def output_option(self, mode):
        return ("output_mode", mode.value)

    def columns_option(self, columns):
        return ("columns", columns)

    def query_option(self, query_str):
        return ("query", literal_eval(query_str.value))

    def column_list(self, *cols):
        return [c.value for c in cols]

    def dot_notation_command(self, alias, action_name, arg_block=None):
        if isinstance(arg_block, dict) or arg_block is None:
            return DotNotationCommand(alias.value, action_name.value, arg_block or {})
        else:
            return PositionalArgActionCommand(alias.value, action_name.value, arg_block)

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

    def query_command(self, cmd_obj):
        return cmd_obj

    def script_command(self, cmd_obj):
        return cmd_obj

    def connection_command(self, cmd_obj):
        return cmd_obj

    def open_command(self, cmd_obj):
        return cmd_obj

    def app_command(self, cmd_obj):
        return cmd_obj

    def app_subcommand(self, cmd_obj):
        return cmd_obj

    def compile_command(self, cmd_obj):
        return cmd_obj

    def process_command(self, cmd_obj):
        return cmd_obj

    def process_subcommand(self, cmd_obj):
        return cmd_obj

    def open_args(self, *args):
        return list(args)

    def open_command_handler(self, open_args=None):
        args = open_args or []
        positional_args = [
            arg.value
            for arg in args
            if hasattr(arg, "type") and arg.type in ("ARG", "JINJA_BLOCK")
        ]
        named_args_list = [arg for arg in args if isinstance(arg, tuple)]
        asset_type = positional_args[0] if positional_args else None
        asset_name = positional_args[1] if len(positional_args) > 1 else None
        args_dict = {key.lstrip("-"): value for key, value in named_args_list}
        return OpenCommand(asset_type, asset_name, args_dict)

    def connection_create(self, *named_args):
        return ConnectionCommand("create", named_args=dict(named_args))

    def compile_command_with_args(self, *named_args):
        return CompileCommand(named_args=dict(named_args))

    def app_install(self, *named_args):
        return AppCommand("install", args=dict(named_args))

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
        return FlowCommand("list", named_args={})

    def query_list(self):
        return QueryCommand("list", named_args={})

    def script_list(self):
        return ScriptCommand("list", named_args={})

    def connection_list(self):
        return ConnectionCommand("list")

    def app_list(self):
        return AppCommand("list", args={})

    def app_uninstall(self, arg):
        return AppCommand("uninstall", args={"id": arg.value})

    def app_sync(self):
        return AppCommand("sync", args={})

    def app_package(self, arg):
        return AppCommand("package", args={"path": arg.value})

    def app_search(self, query=None):
        return AppCommand("search", args={"query": query.value if query else None})

    def process_list(self):
        return ProcessCommand("list")

    def process_logs(self, arg, follow=None):
        return ProcessCommand("logs", arg.value, follow is not None)

    def process_stop(self, arg):
        return ProcessCommand("stop", arg.value)

    def workspace_command(self, cmd_obj):
        return cmd_obj

    def workspace_subcommand(self, cmd_obj):
        return cmd_obj

    def workspace_list(self):
        return WorkspaceCommand("list")

    def workspace_add(self, path):
        return WorkspaceCommand("add", path.value)

    def workspace_remove(self, path):
        return WorkspaceCommand("remove", path.value)

    def workspace_index(self, *named_args):
        # --- FIX: Strip the prefix from the key ---
        return WorkspaceCommand("index", args={k.lstrip("-"): v for k, v in named_args})

    def find_command(self, *args):
        # The *args from Lark will contain all matched items (Tokens and Tuples).
        logger.debug("transformer.find_command.received_args", args=args)

        items = list(args)

        query = next(
            (
                item.value
                for item in items
                if hasattr(item, "type") and item.type == "STRING"
            ),
            None,
        )
        if query:
            query = literal_eval(query)

        # This comprehension correctly handles named arguments (which are tuples)
        named_args = {
            item[0].lstrip("-"): (
                item[1].value if hasattr(item[1], "value") else item[1]
            )
            for item in items
            if isinstance(item, tuple)
        }

        return FindCommand(query=query, args=named_args)

    def _process_run_args(self, *args):
        """
        Processes a list of (key, value) tuples from the parser into a clean
        dictionary suitable for the command managers. It separates structural
        arguments (flags like --name) from user-defined parameters (key=value).
        """
        named_args = {}
        params = {}
        for k, v in args:
            if k.startswith("--"):
                # This is a structural flag (e.g., --name, --on)
                named_args[k.lstrip("-")] = v
            else:
                # This is a user-defined parameter (e.g., status=Booked)
                params[k] = v
        named_args["params"] = params
        return named_args

    def flow_run(self, *args):
        return FlowCommand("run", named_args=self._process_run_args(*args))

    def query_run(self, *args):
        return QueryCommand("run", named_args=self._process_run_args(*args))

    def script_run(self, *args):
        return ScriptCommand("run", named_args=self._process_run_args(*args))

    def arguments(self, *args):
        return dict(args)

    def kv_pair(self, key, value):
        return key.value, value

    def kw_argument(self, key, value):
        return key.value, value

    def named_argument(self, flag, value=None):
        """
        Processes a flag and its optional value.
        Critically, it evaluates STRING tokens to strip quotes.
        """
        final_value = value
        if value is not None:
            # Check if the value is a Lark Token and if its type is STRING
            if hasattr(value, "type") and value.type == "STRING":
                final_value = literal_eval(value.value)
            # Handle other token types that have a .value attribute
            elif hasattr(value, "value"):
                final_value = value.value
        else:
            final_value = True  # Handle boolean flags like --rebuild

        return (flag.value, final_value)

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
    # [Constructor remains unchanged]
    def __init__(self, state: SessionState, output_handler: IOutputHandler):
        self.state = state
        self.output_handler = output_handler
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
        self.workspace_manager = WorkspaceManager()
        self.index_manager = IndexManager()
        self.find_manager = FindManager()
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
        if self._orchestrator is None:
            logger.debug("executor.lazy_load", component="AgentOrchestrator")
            self._orchestrator = AgentOrchestrator(self.state, self)
        return self._orchestrator

    async def execute(
        self, command_text: str, piped_input: Any = None
    ) -> Optional[SessionState]:
        # [This method remains unchanged]
        if not command_text.strip():
            return None
        try:
            tree = self.parser.parse(command_text)
            pipeline_command = self.transformer.transform(tree)
            logger.debug(
                "executor.parsed_pipeline",
                pipeline=[
                    (type(cmd[0]).__name__, cmd[1]) for cmd in pipeline_command.commands
                ],
            )
            first_executable, _ = pipeline_command.commands[0]
            is_assignment = isinstance(first_executable, AssignmentCommand)
            final_result = None
            if is_assignment:
                command_to_run, formatter_options = (
                    first_executable.command_to_run.commands[0]
                )
                raw_result = await self._execute_executable(
                    command_to_run, piped_input=piped_input
                )
                processed_result = self._apply_formatters(raw_result, formatter_options)
                if not (
                    isinstance(processed_result, dict) and "error" in processed_result
                ):
                    self.state.variables[first_executable.var_name] = processed_result
                final_result = f"✓ Variable '{first_executable.var_name}' set."
                last_executable = first_executable
                last_options = {}
            else:
                current_input = piped_input
                for command_to_run, formatter_options in pipeline_command.commands:
                    raw_result = await self._execute_executable(
                        command_to_run, piped_input=current_input
                    )
                    current_input = self._apply_formatters(
                        raw_result, formatter_options
                    )
                    if isinstance(current_input, dict) and "error" in current_input:
                        break
                final_result = current_input
                last_executable, last_options = pipeline_command.commands[-1]
            if isinstance(final_result, SessionState):
                return final_result
            if self.output_handler:
                await self.output_handler.handle_result(
                    final_result, last_executable, last_options
                )
        except Exception as e:
            original_exc = getattr(e, "orig_exc", e)
            logger.error(
                "executor.execute.failed", error=str(original_exc), exc_info=True
            )
            error_result = {"error": f"{type(original_exc).__name__}: {original_exc}"}
            if self.output_handler:
                await self.output_handler.handle_result(error_result, None, None)
        return None

    def _apply_formatters(self, raw_result: Any, formatter_options: Dict) -> Any:
        # [This method remains unchanged]
        if not formatter_options:
            if isinstance(raw_result, dict):
                for key in ["results", "data", "content"]:
                    if key in raw_result:
                        val = raw_result[key]
                        if key == "content":
                            try:
                                val = json.loads(val)
                            except (json.JSONDecodeError, TypeError):
                                pass
                        return val
            return raw_result
        processed_result = raw_result
        if "query" in formatter_options:
            processed_result = jmespath.search(
                formatter_options["query"], processed_result
            )
        return processed_result

    async def _execute_executable(
        self, executable: Any, piped_input: Any = None
    ) -> Any:
        # [This method remains unchanged]
        logger.debug(
            "executor.dispatch.begin",
            executable_type=type(executable).__name__,
            has_piped_input=piped_input is not None,
        )
        if isinstance(executable, VariableLookup):
            if executable.var_name not in self.state.variables:
                raise ValueError(f"Variable '{executable.var_name}' not found.")
            if piped_input is not None:
                raise ValueError("Cannot pipe data into a variable lookup.")
            return self.state.variables[executable.var_name]
        if isinstance(executable, Command):
            is_data_producing_command = getattr(
                executable, "subcommand", None
            ) == "run" or isinstance(
                executable, (DotNotationCommand, PositionalArgActionCommand)
            )
            if is_data_producing_command:
                with console.status("Executing command...", spinner="dots") as status:
                    logger.debug(
                        "executor.run_command.begin",
                        command_type=type(executable).__name__,
                        args=getattr(executable, "named_args", None)
                        or getattr(executable, "args", {}),
                    )
                    if isinstance(executable, FlowCommand):
                        status.update(
                            f"Running flow '{executable.named_args.get('name')}'..."
                        )
                        return await self.flow_manager.run_flow(
                            self.state, self.service, executable.named_args
                        )
                    if isinstance(executable, QueryCommand):
                        status.update(
                            f"Running query '{executable.named_args.get('name')}' on '{executable.named_args.get('on')}'..."
                        )
                        return await self.query_manager.run_query(
                            self.state, self.service, executable.named_args
                        )
                    if isinstance(executable, ScriptCommand):
                        status.update(
                            f"Running script '{executable.named_args.get('name')}'..."
                        )
                        return await self.script_manager.run_script(
                            self.state, self.service, executable.named_args, piped_input
                        )
                    if isinstance(
                        executable, (DotNotationCommand, PositionalArgActionCommand)
                    ):
                        return await executable.execute(
                            self.state, self.service, status, piped_input=piped_input
                        )
            else:
                return await self._dispatch_management_command(
                    executable, piped_input=piped_input
                )
        raise TypeError(f"Cannot execute object of type: {type(executable).__name__}")

    async def _dispatch_management_command(
        self, command: Command, piped_input: Any = None
    ) -> Any:
        # [This method remains unchanged]
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
        if isinstance(command, BuiltinCommand) and command.command == "connections":
            return [
                {"Alias": alias, "Source": source}
                for alias, source in self.state.connections.items()
            ]

        command_prints_own_output = False
        simple_confirmation_message = None
        if isinstance(command, BuiltinCommand):
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
                piped_input=piped_input,
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
        elif isinstance(command, WorkspaceCommand):
            logger.debug(
                "workspace.dispatch.begin",
                subcommand=command.subcommand,
                args=command.args,
            )
            command_prints_own_output = True
            if command.subcommand == "list":
                self.workspace_manager.list_roots()
            elif command.subcommand == "add":
                self.workspace_manager.add_root(command.args["path"])
            elif command.subcommand == "remove":
                self.workspace_manager.remove_root(command.args["path"])
            elif command.subcommand == "index":
                # Now we check for the presence of the 'rebuild' key in the args dict.
                if "rebuild" in command.args:
                    self.index_manager.rebuild_index()
                    console.print("✅ VFS Index rebuild complete.")
                else:
                    console.print(
                        "Incremental indexing not yet implemented. Use `workspace index --rebuild`."
                    )
            return None

        elif isinstance(command, FindCommand):
            # This is a data-producing command
            return self.find_manager.find_assets(
                query=command.query,
                asset_type=command.args.get("type"),
                limit=int(command.args.get("limit", 10)),
            )
        if simple_confirmation_message:
            return simple_confirmation_message
        if command_prints_own_output:
            return None
        return {"status": "success", "message": "Command executed."}

    def execute_help(self, args: List[str]):
        # [Help text remains unchanged]
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
            "... --cx-output table", "Render the final output as a formatted table."
        )
        execution_table.add_row(
            "... --cx-columns <col1,col2>", "Select specific columns for table output."
        )
        execution_table.add_row(
            "... --cx-query <jmespath>",
            "Filter or reshape the final output using a JMESPath query.",
        )
        console.print(execution_table)
        console.print()

    def execute_list_connections(self, args: List[str]):
        if not self.state.connections:
            console.print("No active connections in this session.")
            return
        table = Table(title="[bold green]Active Session Connections[/bold green]")
        table.add_column("Alias", style="cyan", no_wrap=True)
        table.add_column("Source", style="magenta")
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
        try:
            executable_obj, _ = self.transformer.transform(
                self.parser.parse(command_text)
            )
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
            return DryRunResult(
                indicates_failure=False, message="Command is syntactically valid."
            )
        except Exception as e:
            return DryRunResult(
                indicates_failure=True, message=f"Command is invalid. Error: {e}"
            )
