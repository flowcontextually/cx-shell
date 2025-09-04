import pytest
from unittest.mock import AsyncMock
from pathlib import Path

# We import the modules we need to patch
from cx_shell.management import session_manager
from cx_shell.engine.connector import config as connector_config

from cx_shell.interactive.executor import CommandExecutor
from cx_shell.interactive.session import SessionState

# Note: We need to also patch all the other managers that create directories on init.
from cx_shell.management import (
    flow_manager,
    query_manager,
    script_manager,
    connection_manager,
)


@pytest.fixture
def clean_cx_home(tmp_path: Path, monkeypatch):
    """Creates an isolated .cx home and redirects all parts of the app to use it."""
    temp_cx_home = tmp_path / ".cx"

    monkeypatch.setattr(connector_config, "CX_HOME", temp_cx_home)
    monkeypatch.setattr(session_manager, "CX_HOME", temp_cx_home)
    monkeypatch.setattr(flow_manager, "CX_HOME", temp_cx_home)
    monkeypatch.setattr(query_manager, "CX_HOME", temp_cx_home)
    monkeypatch.setattr(script_manager, "CX_HOME", temp_cx_home)
    monkeypatch.setattr(connection_manager, "CX_HOME", temp_cx_home)

    return temp_cx_home


@pytest.fixture
def executor(clean_cx_home):
    """Provides a clean executor with a mocked service for each test."""
    state = SessionState()
    executor = CommandExecutor(state)

    executor.service = AsyncMock()
    executor.connection_manager.create_interactive = AsyncMock()

    return executor


# --- Tests ---


@pytest.mark.asyncio
async def test_connect_command(executor: CommandExecutor):
    """Tests that the 'connect' command correctly updates the session state."""
    executor.service.test_connection.return_value = {"status": "success"}
    await executor.execute("connect user:github --as gh")
    assert "gh" in executor.state.connections
    assert executor.state.connections["gh"] == "user:github"


@pytest.mark.asyncio
async def test_variable_assignment(executor: CommandExecutor):
    """Tests that a command result can be assigned to a variable."""
    executor.state.connections["gh"] = "user:github"

    mock_result = {"user": "test", "id": 123}
    executor.service.engine.run_script_model.return_value = {
        "Interactive getUser": mock_result
    }

    await executor.execute('my_var = gh.getUser(username="test")')

    assert "my_var" in executor.state.variables
    assert executor.state.variables["my_var"] == mock_result


@pytest.mark.asyncio
async def test_session_persistence(executor: CommandExecutor):
    """Tests that a session can be saved and then loaded correctly."""
    # The clean_cx_home fixture already ensures that SESSION_DIR in the
    # session_manager module points to our temporary directory.

    # Set some state
    executor.state.connections["test_alias"] = "user:test_conn"
    executor.state.variables["test_var"] = "hello world"

    # Test saving
    await executor.execute("session save test-persistence")

    # Create a new, empty executor to simulate a restart. It will automatically
    # use the same monkeypatched CX_HOME.
    new_state = SessionState()
    new_executor = CommandExecutor(new_state)

    # Test loading
    loaded_state = await new_executor.execute("session load test-persistence")

    assert isinstance(loaded_state, SessionState)
    assert "test_alias" in loaded_state.connections
    assert loaded_state.connections["test_alias"] == "user:test_conn"
    assert "test_var" in loaded_state.variables
    assert loaded_state.variables["test_var"] == "hello world"
