from abc import ABC

from ..interactive.session import SessionState
from ..engine.connector.service import ConnectorService
from ..data.agent_schemas import AgentConfig


class BaseSpecialistAgent(ABC):
    """
    Abstract base class for a specialist agent in the CARE architecture.
    """

    def __init__(self, state: SessionState, connector_service: ConnectorService):
        """
        Initializes the agent with access to the core services.

        Args:
            state: The current interactive session state.
            connector_service: The service for making blueprint-driven API calls (e.g., to LLMs).
        """
        self.state = state
        self.connector_service = connector_service
        # In a real implementation, this would be loaded and cached from ~/.cx/agents.config.yaml
        self.agent_config: AgentConfig | None = self._load_agent_config()

    def _load_agent_config(self) -> AgentConfig | None:
        """Placeholder for loading and validating agents.config.yaml."""
        # TODO: Implement the actual YAML loading and Pydantic validation.
        return None
