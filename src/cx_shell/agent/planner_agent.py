from typing import List

from .base_agent import BaseSpecialistAgent
from ..data.agent_schemas import PlanStep


class PlannerAgent(BaseSpecialistAgent):
    """
    The Planner Agent is responsible for high-level strategic planning.
    It decomposes a user's goal into a sequence of logical, abstract steps.
    """

    async def generate_plan(self, goal: str, strategic_context: str) -> List[PlanStep]:
        """
        Takes a user goal and context, and returns a structured plan.

        Args:
            goal: The user's high-level goal.
            strategic_context: A formatted string of relevant information from the Context Engine.

        Returns:
            A list of PlanStep objects representing the strategic plan.
        """
        # --- This is a placeholder for the actual LLM call ---
        # A real implementation would:
        # 1. Load the planner's configuration from self.agent_config.
        # 2. Construct a detailed system prompt instructing the LLM to act as a planner
        #    and to output a JSON list of steps.
        # 3. Use self.connector_service.engine.run_script_model to call the configured LLM API.
        # 4. Parse the LLM's JSON response and validate it into a List[PlanStep].

        print(f"[DEBUG] Planner received goal: {goal}")
        print(f"[DEBUG] Planner context:\n{strategic_context}")

        # Mocked response for demonstration purposes:
        if "cpu spike" in goal.lower():
            return [
                PlanStep(step="Check for recent error log spikes in Loki."),
                PlanStep(step="Check for high CPU/Memory usage in Prometheus."),
                PlanStep(
                    step="Correlate findings with recent deployments from GitHub."
                ),
            ]
        else:
            return [
                PlanStep(step=f"Step 1 for goal: {goal}"),
                PlanStep(step=f"Step 2 for goal: {goal}"),
            ]
