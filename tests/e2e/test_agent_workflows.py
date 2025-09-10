# /home/dpwanjala/repositories/cx-shell/tests/e2e/test_agent_workflows.py

import pytest
from pytest_mock import MockerFixture
from unittest.mock import AsyncMock  # Import AsyncMock
from cx_shell.interactive.executor import CommandExecutor
from cx_shell.interactive.session import SessionState
from cx_shell.data.agent_schemas import (
    PlanStep,
    LLMResponse,
    CommandOption,
    AnalystResponse,
    DryRunResult,
)


@pytest.fixture
def executor(clean_cx_home):
    """
    Provides a clean CommandExecutor instance for E2E tests.
    """
    state = SessionState(is_interactive=False)

    # --- THIS IS THE FIX ---
    # Create an AsyncMock for the output handler.
    mock_output_handler = AsyncMock()
    executor_instance = CommandExecutor(state, mock_output_handler)
    # --- END FIX ---

    return executor_instance


@pytest.mark.asyncio
async def test_agent_simple_workflow_e2e(
    executor: CommandExecutor, mocker: MockerFixture
):
    """
    End-to-End Test: Verifies the full agent execution loop for a simple goal.

    This test mocks the responses from all three specialist agents (Planner,
    ToolSpecialist, Analyst) to test the orchestrator's logic in isolation.
    """
    # --- ARRANGE ---

    # --- THIS IS THE CRITICAL FIX ---
    # 1. Mock the connection setup to always return True, preventing the
    #    interactive wizard from being called during the test.
    mocker.patch(
        "cx_shell.interactive.agent_orchestrator.AgentOrchestrator._ensure_agent_connection",
        return_value=True,
    )
    # --- END FIX ---

    # 2. Mock the Planner to return a perfect, simple plan.
    mock_plan = [PlanStep(step="List all saved connections.")]
    mocker.patch.object(
        executor.orchestrator.planner, "generate_plan", return_value=mock_plan
    )

    # 3. Mock the Tool Specialist to return a perfect, valid command.
    mock_command_option = CommandOption(
        cx_command="connection list",
        reasoning="The plan requires listing connections.",
        confidence=0.99,
    )
    mock_llm_response = LLMResponse(command_options=[mock_command_option])
    mocker.patch.object(
        executor.orchestrator.tool_specialist,
        "generate_command",
        return_value=mock_llm_response,
    )

    # 4. Mock the Executor's dry_run to always succeed for this command.
    mock_dry_run_result = DryRunResult(
        indicates_failure=False, message="Command is syntactically valid."
    )
    mocker.patch.object(executor, "dry_run", return_value=mock_dry_run_result)

    # 5. Mock the user's confirmation to automatically say "yes".
    mocker.patch.object(
        executor.orchestrator.prompt_session, "prompt_async", return_value="yes"
    )

    # 6. Mock the Analyst to return a perfect analysis.
    mock_analysis = AnalystResponse(
        belief_update={
            "op": "add",
            "path": "/discovered_facts/note",
            "value": "Task complete.",
        },
        summary_text="The connection list was successfully displayed.",
        indicates_strategic_failure=False,
    )
    mocker.patch.object(
        executor.orchestrator.analyst, "analyze_observation", return_value=mock_analysis
    )

    # 7. Spy on the final, low-level execution method to prove the command ran.
    mock_list_connections = mocker.patch.object(
        executor.connection_manager, "list_connections"
    )

    # --- ACT ---
    await executor.execute('agent "list all my saved connections"')

    # --- ASSERT ---
    # The most important assertion: Was the final command's logic actually executed?
    mock_list_connections.assert_called_once()

    # Verify the agent session was cleaned up correctly from the state.
    assert "_agent_beliefs" not in executor.state.variables
