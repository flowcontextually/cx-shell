from typing import Any
from .base_agent import BaseSpecialistAgent
from ..data.agent_schemas import AnalystResponse


class AnalystAgent(BaseSpecialistAgent):
    """
    Interprets command outputs, updates the belief state, and summarizes the turn.
    """

    async def analyze_observation(
        self, step_goal: str, observation: Any
    ) -> AnalystResponse:
        """
        Takes the result of a command and returns an analysis.

        Args:
            step_goal: The goal of the step that produced the observation.
            observation: The data returned by the CommandExecutor.

        Returns:
            A validated AnalystResponse object with the belief update and summary.
        """
        # TODO: Implement the real LLM call using self.connector_service.
        # The prompt will contain the goal, the observation, and instructions
        # to extract key facts and summarize the outcome.

        print(f"[DEBUG] Analyst received observation for goal: {step_goal}")
        # Mocked response:
        return AnalystResponse(
            belief_update={
                "op": "add",
                "path": "/discovered_facts/log_spike",
                "value": True,
            },
            summary_text=f"Successfully checked for logs related to '{step_goal}' and found a spike.",
        )
