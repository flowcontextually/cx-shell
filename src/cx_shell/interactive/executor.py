import asyncio
import json
from typing import List, Any, Optional
from dataclasses import dataclass
from ast import literal_eval
import jmespath
import structlog

from lark import Lark, Transformer, v_args
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
from ..utils import get_asset_path

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
        logger.debug(
            "Transforming: command_line",
            executable=type(executable).__name__,
            formatter=formatter,
        )
        merged_options = {}
        if formatter:
            for option_dict in formatter:
                merged_options.update(option_dict)
        return executable, merged_options or None

    def executable(self, exec_obj):
        logger.debug("Transforming: executable", result_type=type(exec_obj).__name__)
        return exec_obj

    def single_executable(self, exec_obj):
        logger.debug(
            "Transforming: single_executable", result_type=type(exec_obj).__name__
        )
        return exec_obj

    def pipeline(self, *items):
        logger.debug("Transforming: pipeline", num_items=len(items))
        return PipelineCommand(list(items))

    def assignment(self, var_name, executable):
        logger.debug("Transforming: assignment", var_name=var_name.value)
        return AssignmentCommand(var_name.value, executable)

    def single_command(self, command):
        logger.debug("Transforming: single_command", result_type=type(command).__name__)
        return command

    def builtin_command(self, cmd):
        logger.debug("Transforming: builtin_command", result_type=type(cmd).__name__)
        return cmd

    def variable_lookup(self, var_name):
        logger.debug("Transforming: variable_lookup", var_name=var_name.value)
        return VariableLookup(var_name.value)

    def formatter(self, *options):
        logger.debug("Transforming: formatter", options=options)
        return list(options)

    def formatter_option(self, option):
        logger.debug("Transforming: formatter_option", option=option)
        return option

    def output_option(self, mode):
        logger.debug("Transforming: output_option", mode=mode.value)
        return {"output_mode": mode.value}

    def columns_option(self, columns):
        logger.debug("Transforming: columns_option", columns=columns)
        return {"columns": columns}

    def query_option(self, query_str):
        logger.debug("Transforming: query_option", query=query_str.value)
        return {"query": literal_eval(query_str.value)}

    def column_list(self, *cols):
        logger.debug("Transforming: column_list", cols=[c.value for c in cols])
        return [c.value for c in cols]

    def dot_notation_kw_action(self, alias, action_name, arguments=None):
        logger.debug(
            "Transforming: dot_notation_kw_action",
            alias=alias.value,
            action=action_name.value,
        )
        return DotNotationCommand(alias.value, action_name.value, arguments or {})

    def dot_notation_pos_action(self, alias, action_name, string_arg):
        logger.debug(
            "Transforming: dot_notation_pos_action",
            alias=alias.value,
            action=action_name.value,
        )
        return PositionalArgActionCommand(
            alias.value, action_name.value, literal_eval(string_arg.value)
        )

    def connect_command(self, source, alias):
        logger.debug("Transforming: connect_command")
        return BuiltinCommand(["connect", source.value, "--as", alias.value])

    def connections_command(self):
        logger.debug("Transforming: connections_command")
        return BuiltinCommand(["connections"])

    def help_command(self):
        logger.debug("Transforming: help_command")
        return BuiltinCommand(["help"])

    def inspect_command(self, var_name):
        logger.debug("Transforming: inspect_command", var_name=var_name.value)
        return InspectCommand(var_name.value)

    def compile_command(self, *named_args):
        args_dict = {
            key.lstrip("-").replace("-", "_"): value for key, value in named_args
        }
        return CompileCommand(named_args=args_dict)

    def agent_command(self, goal):
        logger.debug("Transforming: agent_command")
        return AgentCommand(literal_eval(goal.value))

    # --- Grouped Command Pass-throughs ---
    def session_command(self, cmd_obj):
        logger.debug("Transforming: session_command", child_type=type(cmd_obj).__name__)
        return cmd_obj

    def session_subcommand(self, cmd_obj):
        logger.debug(
            "Transforming: _session_subcommand", result_type=type(cmd_obj).__name__
        )
        return cmd_obj

    def variable_command(self, cmd_obj):
        logger.debug(
            "Transforming: variable_command", child_type=type(cmd_obj).__name__
        )
        return cmd_obj

    def variable_subcommand(self, cmd_obj):
        logger.debug(
            "Transforming: _variable_subcommand", result_type=type(cmd_obj).__name__
        )
        return cmd_obj

    def flow_command(self, cmd_obj):
        logger.debug("Transforming: flow_command", child_type=type(cmd_obj).__name__)
        return cmd_obj

    def flow_subcommand(self, cmd_obj):
        logger.debug(
            "Transforming: _flow_subcommand", result_type=type(cmd_obj).__name__
        )
        return cmd_obj

    def query_command(self, cmd_obj):
        logger.debug("Transforming: query_command", child_type=type(cmd_obj).__name__)
        return cmd_obj

    def query_subcommand(self, cmd_obj):
        logger.debug(
            "Transforming: _query_subcommand", result_type=type(cmd_obj).__name__
        )
        return cmd_obj

    def script_command(self, cmd_obj):
        logger.debug("Transforming: script_command", child_type=type(cmd_obj).__name__)
        return cmd_obj

    def script_subcommand(self, cmd_obj):
        logger.debug(
            "Transforming: _script_subcommand", result_type=type(cmd_obj).__name__
        )
        return cmd_obj

    def connection_command(self, cmd_obj):
        logger.debug(
            "Transforming: connection_command", child_type=type(cmd_obj).__name__
        )
        return cmd_obj

    def connection_subcommand(self, cmd_obj):
        logger.debug(
            "Transforming: _connection_subcommand", result_type=type(cmd_obj).__name__
        )
        return cmd_obj

    def app_command(self, cmd_obj):
        logger.debug("Transforming: app_command", child_type=type(cmd_obj).__name__)
        return cmd_obj

    def app_subcommand(self, cmd_obj):
        logger.debug(
            "Transforming: _app_subcommand", result_type=type(cmd_obj).__name__
        )
        return cmd_obj

    def process_command(self, cmd_obj):
        logger.debug("Transforming: process_command", child_type=type(cmd_obj).__name__)
        return cmd_obj

    def process_subcommand(self, cmd_obj):
        logger.debug(
            "Transforming: _process_subcommand", result_type=type(cmd_obj).__name__
        )
        return cmd_obj

    # --- Child Rules that Create Objects ---
    def session_list(self):
        logger.debug("Creating SessionCommand(list)")
        return SessionCommand("list")

    def session_save(self, name):
        logger.debug("Creating SessionCommand(save)")
        return SessionCommand("save", name.value)

    def session_load(self, name):
        logger.debug("Creating SessionCommand(load)")
        return SessionCommand("load", name.value)

    def session_rm(self, name):
        logger.debug("Creating SessionCommand(rm)")
        return SessionCommand("rm", name.value)

    def session_status(self):
        logger.debug("Creating SessionCommand(status)")
        return SessionCommand("status")

    def variable_list(self):
        logger.debug("Creating VariableCommand(list)")
        return VariableCommand("list")

    def variable_rm(self, var_name):
        logger.debug("Creating VariableCommand(rm)")
        return VariableCommand("rm", var_name.value)

    def flow_list(self):
        logger.debug("Creating FlowCommand(list)")
        return FlowCommand("list")

    def flow_run(self, *named_args):
        logger.debug("Creating FlowCommand(run) from named arguments")
        args_dict = dict(named_args)
        flow_name = args_dict.pop("--name", None)
        if not flow_name:
            raise ValueError(
                "`flow run` command requires a `--name <flow_name>` argument."
            )

        # All other arguments are treated as parameters for the flow
        return FlowCommand("run", name=flow_name, args=args_dict)

    def query_list(self):
        logger.debug("Creating QueryCommand(list)")
        return QueryCommand("list")

    def query_run(self, *named_args):
        logger.debug("Creating QueryCommand(run) from named arguments")
        args_dict = dict(named_args)
        query_name = args_dict.pop("--name", None)
        on_alias = args_dict.pop("--on", None)

        if not query_name:
            raise ValueError(
                "`query run` command requires a `--name <query_name>` argument."
            )
        if not on_alias:
            raise ValueError("`query run` command requires an `--on <alias>` argument.")

        return QueryCommand(
            "run", name=query_name, named_args={"on_alias": on_alias, "args": args_dict}
        )

    def script_list(self):
        logger.debug("Creating ScriptCommand(list)")
        return ScriptCommand("list")

    def script_run(self, *named_args):
        logger.debug("Creating ScriptCommand(run) from named arguments")
        args_dict = dict(named_args)
        script_name = args_dict.pop("--name", None)
        if not script_name:
            raise ValueError(
                "`script run` command requires a `--name <script_name>` argument."
            )

        return ScriptCommand("run", name=script_name, args=args_dict)

    def connection_list(self):
        return ConnectionCommand("list")

    def connection_create(self, *named_args):
        args_dict = {key.lstrip("-"): value for key, value in named_args}
        return ConnectionCommand("create", named_args=args_dict)

    def open_command(self, asset_type_token, asset_name_token=None, *named_args):
        logger.debug("Creating OpenCommand")
        asset_type = asset_type_token.value
        asset_name = asset_name_token.value if asset_name_token else None
        args_dict = {key.lstrip("-"): value for key, value in named_args}
        return OpenCommand(asset_type, asset_name, args_dict)

    def app_list(self):
        logger.debug("Creating AppCommand(list)")
        # FIX: Provide the required 'args' dictionary.
        return AppCommand("list", args={})

    def app_install(self, *named_args):
        logger.debug("Creating AppCommand(install) from named arguments")
        args_dict = dict(named_args)
        source_keys = {"--id", "--path", "--url"}
        provided_keys = set(args_dict.keys())
        if len(provided_keys.intersection(source_keys)) != 1:
            raise ValueError(
                "`app install` requires exactly one of --id, --path, or --url."
            )
        return AppCommand("install", args=args_dict)

    def app_uninstall(self, arg):
        logger.debug("Creating AppCommand(uninstall)")
        # FIX: Provide the required 'args' dictionary.
        return AppCommand("uninstall", args={"id": arg.value})

    def app_sync(self):
        logger.debug("Creating AppCommand(sync)")
        # FIX: Provide the required 'args' dictionary.
        return AppCommand("sync", args={})

    def app_package(self, arg):
        logger.debug("Creating AppCommand(package)")
        # FIX: Provide the required 'args' dictionary.
        return AppCommand("package", args={"path": arg.value})

    def app_search(self, query=None):
        logger.debug("Creating AppCommand(search)")
        query_val = query.value if query else None
        # FIX: Provide the required 'args' dictionary.
        return AppCommand("search", args={"query": query_val})

    def process_list(self):
        logger.debug("Creating ProcessCommand(list)")
        return ProcessCommand("list")

    def process_logs(self, arg, follow=None):
        logger.debug("Creating ProcessCommand(logs)")
        return ProcessCommand("logs", arg.value, follow is not None)

    def process_stop(self, arg):
        logger.debug("Creating ProcessCommand(stop)")
        return ProcessCommand("stop", arg.value)

    # --- Argument & Terminal Parsing ---
    def arguments(self, *args):
        logger.debug("Parsing: arguments")
        return dict(args)

    def kw_argument(self, key, value):
        logger.debug("Parsing: kw_argument")
        return key.value, value

    def named_argument(self, flag, value):
        processed_value = value

        # --- THIS IS THE FIX ---
        # If the value is a Token, it's either an ARG, STRING, or NUMBER
        if hasattr(value, "type"):
            # For STRING tokens (which can now be '...' or "..."),
            # use literal_eval to safely un-quote and process escapes.
            if value.type == "STRING":
                processed_value = literal_eval(value.value)
            else:  # For ARG, NUMBER, etc.
                processed_value = value.value
        # --- END FIX ---

        # Try to convert unquoted ARGs to numbers if possible
        try:
            processed_value = int(processed_value)
        except (ValueError, TypeError):
            try:
                processed_value = float(processed_value)
            except (ValueError, TypeError):
                pass  # Keep as string if it's not a number

        logger.debug("Parsing: named_argument", flag=flag.value, value=processed_value)
        return (flag.value, processed_value)

    def value(self, v):
        logger.debug("Parsing: value", value_type=type(v).__name__)
        if hasattr(v, "type"):
            if v.type == "JINJA_BLOCK":
                return v.value
            # Apply the same robust literal_eval here for consistency
            if v.type in ("STRING", "NUMBER"):
                return literal_eval(v.value)
            if v.type == "ARG":
                return v.value  # ARG is already unquoted
            if v.type == "CNAME":
                return v.value
        return v

    def true(self, _):
        logger.debug("Parsing: true")
        return True

    def false(self, _):
        logger.debug("Parsing: false")
        return False

    def null(self, _):
        logger.debug("Parsing: null")
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
        self.process_manager = ProcessManager()
        self.compile_manager = CompileManager()
        self.builtin_commands = {
            "connect": self.execute_connect,
            "connections": self.execute_list_connections,
            "help": self.execute_help,
        }
        # Do not create the orchestrator immediately.
        self._orchestrator: Optional[AgentOrchestrator] = None
        grammar_path = get_asset_path("interactive/grammar/cx.lark")
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
        The main entry point for commands initiated by a HUMAN user from the REPL
        or the top-level CLI. It orchestrates parsing, execution, and printing.
        """
        if not command_text.strip():
            return None
        try:
            parsed_tree = self.parser.parse(command_text)
            executable_obj, formatter_options = self.transformer.transform(parsed_tree)

            # --- FIX: Call the internal executor with is_agent_execution=False ---
            # This tells the execution pipeline that a human is running the command,
            # so it can optimize the output for readability (e.g., suppress JSON).
            result = await self._execute_executable(
                executable_obj, piped_input=piped_input, is_agent_execution=False
            )

            if isinstance(result, SessionState):
                return result

            self._print_result(result, executable_obj, formatter_options)

        except Exception as e:
            original_exc = getattr(e, "orig_exc", e)
            console.print(
                f"[bold red]{type(original_exc).__name__}:[/bold red] {original_exc}"
            )

        return None

    async def _execute_executable(
        self, executable: Any, piped_input: Any = None, is_agent_execution: bool = False
    ) -> Any:
        """
        Recursively executes any command object, tracking whether the command
        was initiated by a human or the agent to tailor the output accordingly.
        """
        # --- Handle Structural/Recursive Executables ---
        if isinstance(executable, PipelineCommand):
            current_input = piped_input
            for item in executable.commands:
                # Propagate the agent context flag through the pipeline
                current_input = await self._execute_executable(
                    item,
                    piped_input=current_input,
                    is_agent_execution=is_agent_execution,
                )
                if isinstance(current_input, dict) and "error" in current_input:
                    break
            return current_input

        if isinstance(executable, AssignmentCommand):
            # Propagate the flag to the command being run
            result = await self._execute_executable(
                executable.command_to_run,
                piped_input,
                is_agent_execution=is_agent_execution,
            )
            if not (isinstance(result, dict) and "error" in result):
                self.state.variables[executable.var_name] = result
            # Don't return the full result object for assignments, just a confirmation.
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

        # --- Dispatch All Command Types ---
        if isinstance(executable, Command):
            # Data-producing commands (e.g., `flow run`, `gh.getUser`) are heavy operations.
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
                        on_alias = executable.named_args.get("on_alias")
                        status.update(
                            f"Running query '{executable.name}' on '{on_alias}'..."
                        )
                        query_args = executable.named_args.get("args", {})
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
                # Lightweight management commands are dispatched here.
                return await self._dispatch_management_command(
                    executable, is_agent_execution
                )

        raise TypeError(f"Cannot execute object of type: {type(executable).__name__}")

    async def _dispatch_management_command(
        self, command: Command, is_agent_execution: bool
    ) -> Any:
        """
        Routes management commands, returning a result tailored to the execution context
        (human-readable for users, machine-readable for the agent).
        """
        command_prints_own_output = False
        simple_confirmation_message = None

        # --- Command Logic Dispatch ---

        if isinstance(command, BuiltinCommand):
            command_prints_own_output = True
            handler = self.builtin_commands.get(command.command)
            if handler:
                await handler(command.args) if asyncio.iscoroutinefunction(
                    handler
                ) else handler(command.args)

        elif isinstance(command, ConnectionCommand):
            if command.subcommand == "list":
                self.connection_manager.list_connections()
                command_prints_own_output = True
            elif command.subcommand == "create":
                await self.connection_manager.create_interactive(
                    command.named_args.get("blueprint")
                )
                command_prints_own_output = True

        elif isinstance(command, AppCommand):
            command_prints_own_output = True
            if command.subcommand == "list":
                await self.app_manager.list_installed_apps()
            elif command.subcommand == "install":
                await self.app_manager.install(command.args)
            elif command.subcommand == "uninstall":
                await self.app_manager.uninstall(command.args["id"])
            elif command.subcommand == "package":
                await self.app_manager.package(command.args["path"])
            elif command.subcommand == "search":
                await self.app_manager.search(command.args["query"])

        elif isinstance(command, (FlowCommand, QueryCommand, ScriptCommand)):
            if command.subcommand == "list":
                command_prints_own_output = True
                if isinstance(command, FlowCommand):
                    self.flow_manager.list_flows()
                elif isinstance(command, QueryCommand):
                    self.query_manager.list_queries()
                elif isinstance(command, ScriptCommand):
                    self.script_manager.list_scripts()

        elif isinstance(command, SessionCommand):
            if command.subcommand in ["list", "status"]:
                command_prints_own_output = True
                if command.subcommand == "list":
                    self.session_manager.list_sessions()
                elif command.subcommand == "status":
                    self.session_manager.show_status(self.state)
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

        elif isinstance(command, VariableCommand):
            if command.subcommand == "list":
                self.variable_manager.list_variables(self.state)
                command_prints_own_output = True
            elif command.subcommand == "rm":
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

        elif isinstance(command, ProcessCommand):
            command_prints_own_output = True
            if command.subcommand == "list":
                self.process_manager.list_processes()
            elif command.subcommand == "logs":
                self.process_manager.get_logs(command.arg, command.follow)

        elif isinstance(command, CompileCommand):
            command_prints_own_output = True
            await self.compile_manager.run_compile(
                spec_source=command.named_args.get("spec-url"),
                name=command.named_args.get("name"),
                version=command.named_args.get("version"),
                namespace=command.named_args.get("namespace", "user"),
            )

        elif isinstance(command, AgentCommand):
            command_prints_own_output = True
            await self.orchestrator.start_session(command.goal)

        elif isinstance(command, InspectCommand):
            # Inspect is special: it returns data to be formatted by _print_result,
            # so it does not handle its own output.
            return await command.execute(self.state, self.service, None)

        # --- Context-Aware Return Logic ---

        if is_agent_execution:
            return {
                "status": "success",
                "message": f"Management command '{type(command).__name__}' executed successfully.",
            }
        else:  # Human execution
            if simple_confirmation_message:
                return simple_confirmation_message
            if command_prints_own_output:
                return None

            # Fallback for any other case
            return {"status": "success", "message": "Command executed."}

    def _print_result(self, result: Any, executable: Any, options: dict | None = None):
        """
        Handles the final rendering of a command's result to the console.
        This method is the final stage of the execution pipeline and is responsible
        for all user-facing output, intelligently suppressing redundant messages.
        """
        # --- Stage 1: Handle Terminal Cases & UX-Specific Suppression ---
        if result is None:
            return

        options = options or {}

        # Suppress the generic success JSON for commands that print their own rich output.
        is_generic_success_message = (
            isinstance(result, dict)
            and result.get("status") == "success"
            and "executed successfully" in result.get("message", "")
        )
        is_list_command = (
            hasattr(executable, "subcommand") and executable.subcommand == "list"
        )
        is_connections_command = (
            isinstance(executable, BuiltinCommand)
            and executable.command == "connections"
        )

        # If the command was a 'list'-style command AND the user has not added any
        # formatters (like --query), we assume the manager has already printed a
        # table, so we suppress the final JSON to avoid clutter.
        if (
            is_generic_success_message
            and (is_list_command or is_connections_command)
            and not options
        ):
            return

        # Handle simple string confirmation messages (e.g., from session save/delete).
        if isinstance(result, str):
            console.print(f"[bold green]✓[/bold green] {result}")
            return

        # Handle the special panel format for the 'inspect' command.
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

        # Handle generic runtime errors that may have been passed through the pipeline.
        if isinstance(result, dict) and "error" in result:
            console.print(f"[bold red]Runtime Error:[/bold red] {result['error']}")
            return

        # --- Stage 2: Data Processing & Filtering ---

        # Intelligently unpack the raw result to get to the core data payload.
        data_to_process = result
        if isinstance(data_to_process, dict):
            if "results" in data_to_process:
                data_to_process = data_to_process["results"]
            elif "data" in data_to_process:
                data_to_process = data_to_process["data"]
            elif "content" in data_to_process:
                try:
                    data_to_process = json.loads(data_to_process["content"])
                except (json.JSONDecodeError, TypeError):
                    pass

        # Apply the JMESPath query to the unpacked data before any rendering.
        data_to_render = data_to_process
        if options.get("query"):
            try:
                data_to_render = jmespath.search(options["query"], data_to_process)
            except Exception as e:
                console.print(f"[bold red]JMESPath Error:[/bold red] {e}")
                return

        # --- Stage 3: Output Rendering ---

        output_mode = options.get("output_mode", "default")

        # Render as a table if requested and if the data is a list of objects.
        if (
            output_mode == "table"
            and isinstance(data_to_render, list)
            and data_to_render
        ):
            if all(isinstance(i, dict) for i in data_to_render):
                table = Table(title="Data View", box=box.ROUNDED, show_lines=True)
                headers = options.get("columns") or list(data_to_render[0].keys())
                for header in headers:
                    table.add_column(str(header), style="cyan", overflow="fold")
                for row in data_to_render:
                    table.add_row(*(str(row.get(h, "")) for h in headers))
                console.print(table)
                return

        # Default to printing pretty JSON for all other data-producing cases.
        # This also handles the generic success message if it wasn't suppressed above.
        try:
            formatted_json = json.dumps(data_to_render, indent=2, default=str)
            syntax = Syntax(formatted_json, "json", theme="monokai", line_numbers=True)
            console.print(syntax)
        except (TypeError, OverflowError):
            # Fallback for non-serializable objects (should be rare).
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
