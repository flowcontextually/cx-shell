from abc import ABC
import yaml

from ..interactive.session import SessionState
from ..engine.connector.service import ConnectorService
from ..data.agent_schemas import AgentConfig
from ..engine.connector.config import CX_HOME

AGENT_CONFIG_FILE = CX_HOME / "agents.config.yaml"


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
        self.agent_config: AgentConfig | None = self._load_agent_config()

    def _load_agent_config(self) -> AgentConfig | None:
        """Loads and validates the agents.config.yaml file."""
        if not AGENT_CONFIG_FILE.exists():
            # If the user's config doesn't exist, we should try to use the
            # default one bundled with the application.
            from ..utils import get_asset_path

            default_config_path = get_asset_path("configs/agents.default.yaml")
            if not default_config_path.exists():
                return None
            config_file_to_load = default_config_path
        else:
            config_file_to_load = AGENT_CONFIG_FILE

        try:
            with open(config_file_to_load, "r") as f:
                config_data = yaml.safe_load(f)
            return AgentConfig.model_validate(config_data)
        except Exception:
            # Silently fail if the config is invalid, the agent will not be able to run.
            return None
