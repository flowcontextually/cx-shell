# /home/dpwanjala/repositories/cx-shell/src/cx_shell/agent/analyst_agent.py

import json
from typing import Any
from pydantic import TypeAdapter, ValidationError
import structlog

from .base_agent import BaseSpecialistAgent
from ..data.agent_schemas import AnalystResponse
from ..engine.connector.utils import safe_serialize
from cx_core_schemas.connector_script import (
    ConnectorScript,
    ConnectorStep,
    RunDeclarativeAction,
)

logger = structlog.get_logger(__name__)

SYSTEM_PROMPT = """
You are the Analyst Agent within the `cx` shell's CARE (Composite Agent Reasoning Engine).
Your role is to be a precise and factual data interpreter.

You will be given the goal of the previous step and the raw `Observation` (the JSON output or error message) from the command that was executed.

Your responsibilities are:
1.  **Analyze the Observation:** Determine if the observation indicates success, failure, or partial progress toward the step's goal.
2.  **Extract Key Facts:** Identify the single most important piece of information from the observation and structure it.
3.  **Update Beliefs:** Formulate a single JSON Patch operation to update the agent's belief state. This should typically be an 'add' or 'replace' operation on the `/discovered_facts` or `/plan/{index}/status` paths.
4.  **Summarize:** Write a concise, one-sentence summary of what happened in this turn.

Your output MUST be a single, valid JSON object that conforms to the following structure:
{
  "belief_update": {
    "op": "add|replace",
    "path": "/path/to/update",
    "value": "extracted_value or new_status"
  },
  "summary_text": "A concise, one-sentence natural language summary of the turn."
}
Do not output anything other than this JSON object.
"""


class AnalystAgent(BaseSpecialistAgent):
    """
    Interprets command outputs, updates the belief state, and summarizes the turn.
    """

    async def analyze_observation(
        self, step_goal: str, observation: Any
    ) -> AnalystResponse:
        """
        Takes the result of a command and returns an analysis.
        """
        if not self.agent_config:
            raise RuntimeError("Agent configuration is missing or invalid.")

        profile = self.agent_config.profiles[self.agent_config.default_profile]
        config = profile.analyst

        try:
            observation_str = json.dumps(safe_serialize(observation), indent=2)
        except Exception:
            observation_str = repr(observation)

        user_prompt = f"## Step Goal\n{step_goal}\n\n## Raw Observation\n```json\n{observation_str[:4000]}```"

        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ]

        script = ConnectorScript(
            name="Invoke Analyst Agent",
            steps=[
                ConnectorStep(
                    id="call_analyst_llm",
                    name="Call LLM",
                    connection_source=self.state.connections[config.connection_alias],
                    run=RunDeclarativeAction(
                        action="run_declarative_action",
                        template_key=config.action,
                        context={"messages": messages, **config.parameters},
                    ),
                )
            ],
        )

        result = await self.connector_service.engine.run_script_model(script)
        llm_response_data = result.get("Call LLM", {})

        response_content = (
            llm_response_data.get("choices", [{}])[0]
            .get("message", {})
            .get("content", "{}")
        )

        try:
            response_json = json.loads(response_content)
            adapter = TypeAdapter(AnalystResponse)
            return adapter.validate_python(response_json)
        except (json.JSONDecodeError, ValidationError) as e:
            logger.error(
                "LLM failed to produce valid JSON for analyst response",
                error=str(e),
                raw_content=response_content,
            )
            return AnalystResponse(
                belief_update={
                    "op": "add",
                    "path": "/discovered_facts/analyst_error",
                    "value": f"Failed to parse Analyst response: {e}",
                },
                summary_text="The Analyst agent failed to process the observation.",
            )
