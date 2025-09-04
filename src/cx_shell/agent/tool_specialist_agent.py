from typing import List, Dict, Any

from .base_agent import BaseSpecialistAgent
from ..data.agent_schemas import LLMResponse


class ToolSpecialistAgent(BaseSpecialistAgent):
    """
    Translates a single, concrete plan step into an executable `cx` command.
    """

    async def generate_command(
        self, step_goal: str, tactical_context: List[Dict[str, Any]]
    ) -> LLMResponse:
        """
        Takes a single plan step and tool schemas, and returns a command.

        Args:
            step_goal: The natural language goal of the current step.
            tactical_context: The JSON Schema definitions of available tools.

        Returns:
            A validated LLMResponse object with the proposed command.
        """
        # TODO: Implement the real LLM call using self.connector_service.
        # The prompt will instruct the LLM to act as an expert `cx` user and
        # use the provided tool functions to achieve the step_goal.

        print(f"[DEBUG] Tool Specialist received goal: {step_goal}")
        # Mocked response:
        return LLMResponse(
            reasoning=f"To achieve '{step_goal}', I will use the query action on the loki connection.",
            cx_command=f'loki.query(\'{{pod=~"checkout-api-.*"}}\ | logfmt | level="error"\') # for step: {step_goal}',
        )
