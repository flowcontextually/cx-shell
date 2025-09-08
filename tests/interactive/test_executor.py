import pytest
from unittest.mock import AsyncMock
from cx_shell.interactive.executor import CommandExecutor
from cx_shell.interactive.session import SessionState


@pytest.fixture
def executor(clean_cx_home):
    """Provides a clean executor with a mocked service for each test."""
    state = SessionState(is_interactive=False)  # Use non-interactive for tests
    executor_instance = CommandExecutor(state)

    # Mock the service layer to isolate the executor's parsing and state management logic.
    executor_instance.service = AsyncMock()
    executor_instance.connection_manager.create_interactive = AsyncMock()

    return executor_instance


@pytest.mark.asyncio
async def test_executor_connect_command_updates_state(executor: CommandExecutor):
    """Unit Test: Verifies the 'connect' command correctly updates session state."""
    executor.service.test_connection.return_value = {"status": "success"}

    await executor.execute("connect user:github --as gh")

    assert "gh" in executor.state.connections
    assert executor.state.connections["gh"] == "user:github"


@pytest.mark.asyncio
async def test_executor_variable_assignment(executor: CommandExecutor):
    """Unit Test: Verifies a command result can be assigned to a variable."""
    executor.state.connections["gh"] = "user:github"
    mock_result = {"user": "test", "id": 123}
    executor.service.engine.run_script_model.return_value = {
        "Interactive getUser": mock_result
    }

    await executor.execute('my_var = gh.getUser(username="test")')

    assert "my_var" in executor.state.variables
    assert executor.state.variables["my_var"] == mock_result


@pytest.mark.asyncio
async def test_executor_session_persistence(executor: CommandExecutor):
    """Integration Test: Verifies that a session can be saved and then loaded correctly."""
    # Arrange: Set some state.
    executor.state.connections["test_alias"] = "user:test_conn"
    executor.state.variables["test_var"] = "hello world"

    # Act: Save the session.
    await executor.execute("session save test-persistence")

    # Arrange: Create a new, empty executor to simulate a restart.
    new_executor = CommandExecutor(SessionState(is_interactive=False))

    # Act: Load the session.
    loaded_state = await new_executor.execute("session load test-persistence")

    # Assert: Verify the state was restored.
    assert isinstance(loaded_state, SessionState)
    assert loaded_state.connections.get("test_alias") == "user:test_conn"
    assert loaded_state.variables.get("test_var") == "hello world"
