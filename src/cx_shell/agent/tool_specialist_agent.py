# /home/dpwanjala/repositories/cx-shell/src/cx_shell/agent/tool_specialist_agent.py

import json
from typing import List, Dict, Any
from pydantic import TypeAdapter, ValidationError
import structlog

from .base_agent import BaseSpecialistAgent
from ..data.agent_schemas import LLMResponse
from cx_core_schemas.connector_script import (
    ConnectorScript,
    ConnectorStep,
    RunDeclarativeAction,
)

logger = structlog.get_logger(__name__)

TRANSLATE_SYSTEM_PROMPT = """
You are an expert `cx` shell command-line co-pilot. Your sole purpose is to translate a user's natural language goal into a single, best-effort `cx` command.
The user's prompt will be their goal.
You will be provided with a JSON list of available tool functions.
Analyze the user's goal and the available tools, and construct the single most appropriate `cx` command to achieve it.
Your output MUST be only the single line of `cx` shell command text. Do not add any explanation, formatting, or backticks.
"""

TOOL_SPECIALIST_SYSTEM_PROMPT = """
You are the Tool Specialist Agent within the `cx` shell's CARE (Composite Agent Reasoning Engine).
Your role is to act as a precise, deterministic, and safe tool-using component.
You will be given a single, high-level task from the Planner Agent.
You will also be provided with a JSON list of available tool functions and their schemas that you can use to accomplish this task.
Your responsibilities are:
1.  **Analyze the Task:** Understand the goal of the current step.
2.  **Select the Best Tool:** Choose the single most appropriate tool or pipeline of tools from the provided list.
3.  **Construct the Command:** Formulate a syntactically correct `cx` shell command to execute the chosen tool with the correct parameters. You can use pipelining (`|`) and session variables (`=`) if it is more efficient.
4.  **Reason:** Briefly explain your choice of command in the `reasoning` field.
Your output MUST be a single, valid JSON object that conforms to the following structure:
{
  "reasoning": "A brief explanation of your thought process and command choice.",
  "cx_command": "The complete, single-line `cx` command to be executed."
}
Do not output anything other than this JSON object.
"""


class ToolSpecialistAgent(BaseSpecialistAgent):
    """
    Translates a single, concrete plan step into an executable `cx` command.
    Also serves the stateless "Translate" (`//`) functionality.
    """

    async def generate_command(
        self,
        step_goal: str,
        tactical_context: List[Dict[str, Any]],
        is_translate: bool = False,
    ) -> LLMResponse:
        """
        Takes a single plan step and tool schemas, and returns a command.
        """
        if not self.agent_config:
            raise RuntimeError(
                "Agent configuration is missing or invalid. Please run `cx init` or check `~/.cx/agents.config.yaml`."
            )

        profile = self.agent_config.profiles[self.agent_config.default_profile]
        config = profile.co_pilot if is_translate else profile.tool_specialist

        system_prompt = (
            TRANSLATE_SYSTEM_PROMPT if is_translate else TOOL_SPECIALIST_SYSTEM_PROMPT
        )

        tools_str = json.dumps(tactical_context, indent=2)
        user_prompt = f"## Goal\n{step_goal}\n\n## Available Tools\n{tools_str}"

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ]

        script = ConnectorScript(
            name="Invoke Tool Specialist Agent",
            steps=[
                ConnectorStep(
                    id="call_tool_specialist_llm",
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
        # logger.info("Raw response from LLM provider", response_data=llm_response_data)

        response_content = ""
        try:
            response_content = (
                llm_response_data.get("choices", [{}])[0]
                .get("message", {})
                .get("content", "")
            )
        except (IndexError, AttributeError) as e:
            logger.error(
                "Failed to parse LLM response structure",
                error=str(e),
                raw_response=llm_response_data,
            )

        if is_translate:
            return LLMResponse(
                reasoning="Translate suggestion", cx_command=response_content.strip()
            )

        try:
            response_json = json.loads(response_content)
            adapter = TypeAdapter(LLMResponse)
            return adapter.validate_python(response_json)
        except (json.JSONDecodeError, ValidationError) as e:
            logger.error(
                "LLM failed to produce valid JSON for agent response",
                error=str(e),
                raw_content=response_content,
            )
            return LLMResponse(
                reasoning=f"LLM failed to produce a valid JSON response. Error: {e}. Raw content: {response_content}",
                cx_command=None,
            )
