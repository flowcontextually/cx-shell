import json
from pathlib import Path
from datetime import datetime, timezone

from .engine.connector.config import CX_HOME

FEEDBACK_LOG_FILE = CX_HOME / "feedback_log.jsonl"


class FeedbackLogger:
    """
    A fire-and-forget logger for capturing events that can be used for
    long-term agent improvement (fine-tuning, pattern mining).
    """

    def _log(self, event_type: str, data: dict):
        """Appends a structured, timestamped event to the feedback log file."""
        log_entry = {
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "event_type": event_type,
            "data": data,
        }
        with open(FEEDBACK_LOG_FILE, "a") as f:
            f.write(json.dumps(log_entry) + "\n")

    def log_user_correction(self, intent: str, agent_command: str, user_command: str):
        """
        Logs an instance where a user rejected an agent's command and provided
        a successful alternative. This is a high-value data point for fine-tuning.

        Args:
            intent: The high-level goal for the current plan step.
            agent_command: The incorrect command proposed by the agent.
            user_command: The correct command provided by the user.
        """
        self._log(
            "user_correction",
            {
                "intent": intent,
                "agent_command": agent_command,
                "user_command": user_command,
            },
        )

    def log_successful_pattern(self, goal: str, final_flow_path: Path):
        """
        Logs a successful agentic session, linking the original high-level goal
        to the durable, reusable flow file that was generated.

        Args:
            goal: The original, top-level user goal.
            final_flow_path: The path to the .flow.yaml file that solves the goal.
        """
        self._log(
            "successful_pattern",
            {
                "goal": goal,
                "solution_asset_path": str(final_flow_path),
                "solution_flow_content": final_flow_path.read_text(),
            },
        )
