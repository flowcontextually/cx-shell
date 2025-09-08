# /home/dpwanjala/repositories/cx-shell/src/cx_shell/agent/base_agent.py

from abc import ABC
import yaml
from typing import Optional

from ..interactive.session import SessionState
from .llm_client import LLMClient
from ..data.agent_schemas import AgentConfig
from ..engine.connector.config import CX_HOME

AGENT_CONFIG_FILE = CX_HOME / "agents.config.yaml"


class BaseSpecialistAgent(ABC):
    """Abstract base class for a specialist agent in the CARE architecture."""

    def __init__(self, state: SessionState, llm_client: LLMClient):
        """
        Initializes the agent with access to the core services.
        Crucially, this constructor performs NO I/O.
        """
        self.state = state
        self.llm_client = llm_client
        self._agent_config: Optional[AgentConfig] = None
        self._config_loaded = False

    @property
    def agent_config(self) -> Optional[AgentConfig]:
        """
        Lazily loads the agent configuration on first access.
        This prevents blocking I/O in the constructor.
        """
        if not self._config_loaded:
            self._agent_config = self._load_agent_config()
            self._config_loaded = True
        return self._agent_config

    def _load_agent_config(self) -> Optional[AgentConfig]:
        """Loads and validates the agents.config.yaml file."""
        from ..utils import get_asset_path

        config_file_to_load = AGENT_CONFIG_FILE
        if not config_file_to_load.exists():
            default_config_path = get_asset_path("configs/agents.default.yaml")
            if not default_config_path.exists():
                return None
            config_file_to_load = default_config_path

        try:
            with open(config_file_to_load, "r") as f:
                config_data = yaml.safe_load(f)
            return AgentConfig.model_validate(config_data)
        except Exception:
            return None
