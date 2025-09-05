# /home/dpwanjala/repositories/cx-shell/src/cx_shell/interactive/commands.py

from abc import ABC
from pathlib import Path
from typing import Any, List, Dict

# Rich imports are only for type hinting, not for direct use.
from rich.status import Status

import yaml

# Local application imports
from .session import SessionState
from ..engine.connector.service import ConnectorService
from ..engine.transformer.service import TransformerService
from ..engine.connector.config import CX_HOME

from cx_core_schemas.connector_script import (
    ConnectorScript,
    ConnectorStep,
    RunDeclarativeAction,
    RunSqlQueryAction,
    BrowsePathAction,
    ReadContentAction,
)

SESSION_DIR = CX_HOME / "sessions"


def create_script_for_step(step: ConnectorStep) -> ConnectorScript:
    """Helper function to wrap a single step in a script object."""
    return ConnectorScript(name="Interactive Script", steps=[step])


class Command(ABC):
    """Abstract base class for all executable REPL commands."""

    async def execute(
        self,
        state: SessionState,
        service: ConnectorService,
        status: Status,
        piped_input: Any = None,
    ) -> Any:
        """Executes the command and returns a result."""
        # Piped input is added to the signature for commands that can receive it.
        # Most commands will ignore it.
        raise NotImplementedError


class DotNotationCommand(Command):
    """Represents a command like `gh.getUser(username="torvalds")`."""

    def __init__(self, alias: str, action_name: str, kwargs: Dict[str, Any]):
        self.alias = alias
        self.action_name = action_name
        self.kwargs = kwargs

    def to_step(self, state: SessionState) -> ConnectorStep:
        if self.alias not in state.connections:
            raise ValueError(f"Unknown connection alias '{self.alias}'.")
        connection_source = state.connections[self.alias]
        return ConnectorStep(
            id=f"interactive_{self.action_name}",
            name=f"Interactive {self.action_name}",
            connection_source=connection_source,
            run=RunDeclarativeAction(
                action="run_declarative_action",
                template_key=self.action_name,
                context=self.kwargs,
            ),
        )

    async def execute(
        self,
        state: SessionState,
        service: ConnectorService,
        status: Status,
        piped_input: Any = None,
    ) -> Any:
        status.update(
            f"Executing [cyan]{self.alias}[/cyan].[yellow]{self.action_name}[/yellow]([magenta]{self.kwargs or ''}[/magenta])..."
        )
        step = self.to_step(state)
        script = create_script_for_step(step)
        results = await service.engine.run_script_model(
            script, session_variables=state.variables
        )
        return results.get(step.name)


class PositionalArgActionCommand(Command):
    """Represents a command with a single positional arg, like `db.query("...")`."""

    def __init__(self, alias: str, action_name: str, arg: Any):
        self.alias = alias
        self.action_name = action_name
        self.arg = arg

    def to_step(self, state: SessionState) -> ConnectorStep:
        if self.alias not in state.connections:
            raise ValueError(f"Unknown connection alias '{self.alias}'.")
        connection_source = state.connections[self.alias]
        run_action = None
        if self.action_name == "query":
            run_action = RunSqlQueryAction(
                action="run_sql_query", query=self.arg, parameters={}
            )
        elif self.action_name == "browse":
            run_action = BrowsePathAction(action="browse_path", path=self.arg)
        elif self.action_name == "read":
            run_action = ReadContentAction(action="read_content", path=self.arg)
        else:
            raise NotImplementedError(
                f"Positional argument action '{self.action_name}' is not implemented in to_step method."
            )
        return ConnectorStep(
            id=f"interactive_{self.action_name}",
            name=f"Interactive {self.action_name}",
            connection_source=connection_source,
            run=run_action,
        )

    async def execute(
        self,
        state: SessionState,
        service: ConnectorService,
        status: Status,
        piped_input: Any = None,
    ) -> Any:
        status.update(
            f"Executing [cyan]{self.alias}[/cyan].[yellow]{self.action_name}[/yellow]([magenta]'{self.arg}'[/magenta])..."
        )
        step = self.to_step(state)
        script = create_script_for_step(step)
        results = await service.engine.run_script_model(
            script, session_variables=state.variables
        )
        return results.get(step.name)


class BuiltinCommand(Command):
    """Represents a built-in command like `connect` or `help`."""

    def __init__(self, parts: List[str]):
        self.command = parts[0].lower() if parts else ""
        self.args = parts[1:] if len(parts) > 1 else []

    async def execute(
        self,
        state: SessionState,
        service: ConnectorService,
        status: Status,
        piped_input: Any = None,
    ) -> Any:
        raise NotImplementedError(
            "BuiltinCommand execution is handled by CommandExecutor."
        )


class AssignmentCommand(Command):
    """Represents a variable assignment."""

    def __init__(self, var_name: str, command_to_run: Command):
        self.var_name = var_name
        self.command_to_run = command_to_run

    async def execute(
        self,
        state: SessionState,
        service: ConnectorService,
        status: Status,
        piped_input: Any = None,
    ) -> Any:
        # The executor is responsible for calling the RHS command and assigning the variable.
        # This method is a placeholder to satisfy the ABC.
        raise NotImplementedError(
            "AssignmentCommand execution is handled by CommandExecutor."
        )


class InspectCommand(Command):
    """Represents a variable inspection, e.g., `my_var?`."""

    def __init__(self, var_name: str):
        self.var_name = var_name

    async def execute(
        self,
        state: SessionState,
        service: ConnectorService,
        status: Status,
        piped_input: Any = None,
    ) -> Any:
        if self.var_name not in state.variables:
            raise ValueError(
                f"Variable '{self.var_name}' not found in current session."
            )
        obj = state.variables[self.var_name]
        summary = {"var_name": self.var_name, "type": type(obj).__name__}
        if isinstance(obj, (list, tuple, set)):
            summary["length"] = len(obj)
            if obj:
                first_item = next(iter(obj))
                if isinstance(first_item, dict):
                    summary["item_zero_keys"] = list(first_item.keys())
                else:
                    summary["item_zero_preview"] = repr(first_item)
        elif isinstance(obj, dict):
            summary["length"] = len(obj)
            summary["keys"] = list(obj.keys())
        else:
            summary["value_preview"] = repr(obj)
        return summary


class PipelineCommand(Command):
    """Represents a series of commands chained by pipes. Acts as a data container."""

    def __init__(self, commands: List[Command]):
        self.commands = commands

    async def execute(
        self,
        state: SessionState,
        service: ConnectorService,
        status: Status,
        piped_input: Any = None,
    ) -> Any:
        raise NotImplementedError(
            "PipelineCommand execution is handled by CommandExecutor."
        )


class ScriptedCommand(Command):
    """Represents a command that runs a YAML script, like `transform run`."""

    def __init__(self, command_type: str, script_path: str):
        self.command_type = command_type
        self.script_path = script_path

    async def execute(
        self,
        state: SessionState,
        service: ConnectorService,
        status: Status,
        piped_input: Any = None,
    ) -> Any:
        expanded_path = Path(self.script_path).expanduser().resolve()
        if not expanded_path.exists():
            raise FileNotFoundError(f"Script not found at: {expanded_path}")
        with open(expanded_path, "r", encoding="utf-8") as f:
            script_data = yaml.safe_load(f)

        if self.command_type == "transform":
            transformer = TransformerService()
            run_context = {"initial_input": piped_input, **state.variables}
            return await transformer.run(script_data, run_context)

        raise NotImplementedError(
            f"Scripted command '{self.command_type}' not implemented."
        )


class SessionCommand(Command):
    """Represents a session management command, e.g., `session save my-session`."""

    def __init__(self, subcommand: str, arg: str | None = None):
        self.subcommand = subcommand
        self.arg = arg

    async def execute(
        self,
        state: SessionState,
        service: ConnectorService,
        status: Status,
        piped_input: Any = None,
    ) -> Any:
        # This command is special and will be handled directly by the executor,
        # which will instantiate and use the SessionManager.
        raise NotImplementedError(
            "SessionCommand execution is handled by CommandExecutor."
        )


class VariableCommand(Command):
    """Represents a variable management command, e.g., `var list` or `var rm my_var`."""

    def __init__(self, subcommand: str, arg: str | None = None):
        self.subcommand = subcommand
        self.arg = arg

    async def execute(
        self,
        state: SessionState,
        service: ConnectorService,
        status: Status,
        piped_input: Any = None,
    ) -> Any:
        # This is a synchronous, built-in style command.
        # The executor will handle it directly.
        raise NotImplementedError(
            "VariableCommand execution is handled by CommandExecutor."
        )


class FlowCommand(Command):
    """Represents a flow management command, e.g., `flow list` or `flow run my-flow`."""

    def __init__(
        self,
        subcommand: str,
        name: str | None = None,
        args: Dict[str, Any] | None = None,
    ):
        self.subcommand = subcommand
        self.name = name
        self.args = args or {}

    async def execute(
        self,
        state: SessionState,
        service: ConnectorService,
        status: Status,
        piped_input: Any = None,
    ) -> Any:
        raise NotImplementedError(
            "FlowCommand execution is handled by CommandExecutor."
        )


class QueryCommand(Command):
    """Represents a query management command, e.g., `query run --on db my-query`."""

    def __init__(
        self,
        subcommand: str,
        name: str | None = None,
        on_alias: str | None = None,
        args: Dict[str, Any] | None = None,
    ):
        self.subcommand = subcommand
        self.name = name
        self.on_alias = on_alias
        self.args = args or {}

    async def execute(
        self,
        state: SessionState,
        service: ConnectorService,
        status: Status,
        piped_input: Any = None,
    ) -> Any:
        """
        This method is a placeholder to satisfy the abstract base class.
        The actual logic is handled by the CommandExecutor, which delegates to
        the appropriate QueryManager method based on the subcommand.
        """
        raise NotImplementedError(
            "QueryCommand execution is handled by the CommandExecutor's dispatch logic."
        )


class ConnectionCommand(Command):
    """Represents a connection management command, e.g., `connection list`."""

    def __init__(self, subcommand: str, blueprint: str | None = None):
        self.subcommand = subcommand
        self.blueprint = blueprint

    async def execute(
        self,
        state: SessionState,
        service: ConnectorService,
        status: Status,
        piped_input: Any = None,
    ) -> Any:
        """
        This method is a placeholder to satisfy the abstract base class.
        The actual logic is handled synchronously by the CommandExecutor, which
        delegates to the ConnectionManager based on the subcommand.
        """
        raise NotImplementedError(
            "ConnectionCommand execution is handled by the CommandExecutor's dispatch logic."
        )


class ScriptCommand(Command):
    """Represents a script management command, e.g., `script run my-script`."""

    def __init__(
        self,
        subcommand: str,
        name: str | None = None,
        args: Dict[str, Any] | None = None,
    ):
        self.subcommand = subcommand
        self.name = name
        self.args = args or {}

    async def execute(
        self,
        state: SessionState,
        service: ConnectorService,
        status: Status,
        piped_input: Any = None,
    ) -> Any:
        raise NotImplementedError(
            "ScriptCommand execution is handled by CommandExecutor."
        )


class OpenCommand(Command):
    """Represents the `open` command for assets."""

    def __init__(
        self,
        asset_type: str,
        asset_name: str | None = None,
        handler: str | None = None,
        on_alias: str | None = None,
    ):
        self.asset_type = asset_type
        self.asset_name = asset_name
        self.handler = handler or "default"
        self.on_alias = on_alias

    async def execute(
        self,
        state: SessionState,
        service: ConnectorService,
        status: Status,
        piped_input: Any = None,
    ) -> Any:
        raise NotImplementedError(
            "OpenCommand execution is handled by the CommandExecutor."
        )


class AppCommand(Command):
    """Represents an application management command, e.g., `app install ...`."""

    def __init__(self, subcommand: str, arg: str | None = None):
        self.subcommand = subcommand
        self.arg = arg

    async def execute(
        self,
        state: SessionState,
        service: ConnectorService,
        status: Status,
        piped_input: Any = None,
    ) -> Any:
        # This will be handled directly by the CommandExecutor's dispatch logic
        raise NotImplementedError


class AgentCommand(Command):
    """Represents an agent invocation command, e.g., `agent 'do something'`."""

    def __init__(self, goal: str):
        self.goal = goal

    async def execute(
        self,
        state: SessionState,
        service: ConnectorService,
        status: Status,
        piped_input: Any = None,
    ) -> Any:
        raise NotImplementedError


class ProcessCommand(Command):
    """Represents a background process management command, e.g., `process list`."""

    def __init__(self, subcommand: str, arg: str | None = None, follow: bool = False):
        self.subcommand = subcommand
        self.arg = arg
        self.follow = follow

    async def execute(
        self,
        state: SessionState,
        service: ConnectorService,
        status: Status,
        piped_input: Any = None,
    ) -> Any:
        raise NotImplementedError
