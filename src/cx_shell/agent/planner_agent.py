from typing import List
import json
from pydantic import TypeAdapter

from .base_agent import BaseSpecialistAgent
from ..data.agent_schemas import PlanStep
from cx_core_schemas.connector_script import (
    ConnectorScript,
    ConnectorStep,
    RunDeclarativeAction,
)

SYSTEM_PROMPT = """
You are the Planner Agent, a high-level strategic thinker for the `cx` shell.
Your role is to decompose a user's complex goal into a logical, step-by-step plan.
Do not generate commands. Focus only on the strategy.

You will be given the user's goal and the current strategic context, which includes
relevant existing workflows and available connections.

Your output MUST be a JSON array of objects, where each object represents a step in the plan.
Each object must have a single key: "step", with a string value describing the goal for that step.
Example: [{"step": "First step goal."}, {"step": "Second step goal."}]
"""


class PlannerAgent(BaseSpecialistAgent):
    """
    The Planner Agent is responsible for high-level strategic planning.
    It decomposes a user's goal into a sequence of logical, abstract steps.
    """

    async def generate_plan(self, goal: str, strategic_context: str) -> List[PlanStep]:
        """
        Takes a user goal and context, and returns a structured plan.
        """
        if not self.agent_config:
            raise RuntimeError("Agent configuration is missing or invalid.")

        profile = self.agent_config.profiles[self.agent_config.default_profile]
        planner_config = profile.planner

        user_prompt = (
            f"## User Goal\n{goal}\n\n## Strategic Context\n{strategic_context}"
        )

        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ]

        # Use the cx ConnectorService to make the LLM call
        script = ConnectorScript(
            name="Invoke Planner Agent",
            steps=[
                ConnectorStep(
                    id="call_planner_llm",
                    name="Call Planner LLM",
                    connection_source=self.state.connections[
                        planner_config.connection_alias
                    ],
                    run=RunDeclarativeAction(
                        action="run_declarative_action",
                        template_key=planner_config.action,
                        context={"messages": messages, **planner_config.parameters},
                    ),
                )
            ],
        )

        result = await self.connector_service.engine.run_script_model(script)
        llm_response = result["Call Planner LLM"]

        # Extract, parse, and validate the plan from the LLM's response
        # This logic needs to be provider-specific (OpenAI, Anthropic, etc.)
        # For now, we assume an OpenAI-like response structure.
        response_content = (
            llm_response.get("choices", [{}])[0].get("message", {}).get("content", "[]")
        )

        try:
            plan_data = json.loads(response_content)
            adapter = TypeAdapter(List[PlanStep])
            return adapter.validate_python(plan_data)
        except (json.JSONDecodeError, Exception):
            # Fallback or error handling
            return [PlanStep(step="Failed to generate a valid plan.")]
