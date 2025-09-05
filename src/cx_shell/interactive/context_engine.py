import sqlite3
import importlib.util
from pydantic import BaseModel
# We will assume LanceDB and an embedding model are installed.
# For now, these imports are placeholders to show intent.
# import lancedb
# from sentence_transformers import SentenceTransformer

from pathlib import Path
from typing import List, Dict, Any

from ..engine.connector.config import CX_HOME, ConnectionResolver
from ..interactive.session import SessionState
from ..data.agent_schemas import AgentBeliefs

# --- Constants ---
CONTEXT_DIR = CX_HOME / "context"
HISTORY_DB_FILE = CONTEXT_DIR / "history.sqlite"
VECTOR_STORE_DIR = CONTEXT_DIR / "vector.lance"


class DynamicContextEngine:
    """
    Constructs intelligent, minimal, and relevant context for the CARE agents.
    It combines vector search, structured queries, and graph traversal
    to provide a rich understanding of the user's workspace and history.
    """

    def __init__(self, state: SessionState):
        CONTEXT_DIR.mkdir(parents=True, exist_ok=True)
        self.state = state
        self.resolver = ConnectionResolver()
        # In a real implementation, these would be initialized carefully.
        # self.embedding_model = SentenceTransformer('all-MiniLM-L6-v2')
        # self.db = lancedb.connect(VECTOR_STORE_DIR)
        self._init_history_db()

    def _init_history_db(self):
        """Initializes the SQLite database for structured event history."""
        with sqlite3.connect(HISTORY_DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                event_type TEXT NOT NULL,
                actor TEXT NOT NULL,
                command_text TEXT,
                status TEXT,
                duration_ms INTEGER,
                observation_summary TEXT,
                session_id TEXT
            )
            """)
            conn.commit()

    def get_strategic_context(self, goal: str, beliefs: AgentBeliefs) -> str:
        """
        Builds a high-level context for the PlannerAgent.
        It retrieves relevant past workflows and high-level tool categories.

        Args:
            goal: The user's current high-level goal.
            beliefs: The current state of the agent's beliefs.

        Returns:
            A formatted string to be injected into the Planner's system prompt.
        """
        # --- Placeholder for RAG implementation ---
        # 1. Embed the `goal`.
        # 2. Query AssetVectorStore for similar flows/blueprints.
        # 3. Query SessionHistoryDB for successful commands related to the goal.
        # 4. Query the GraphDB for relationships.
        # --- End Placeholder ---

        context_parts = []
        context_parts.append("## Current Situation")
        context_parts.append(f'- The user\'s goal is: "{goal}"')
        if beliefs.plan:
            context_parts.append("- The current plan is:")
            for i, step in enumerate(beliefs.plan):
                status_icon = (
                    "✓"
                    if step.status == "completed"
                    else ("✗" if step.status == "failed" else "…")
                )
                context_parts.append(f"  {status_icon} {i + 1}. {step.step}")

        context_parts.append("\n## Available Connections")
        if not self.state.connections:
            context_parts.append("- No connections are active.")
        else:
            for alias in self.state.connections.keys():
                context_parts.append(f"- `{alias}`: An active connection.")

        # TODO: Add retrieved RAG results here

        return "\n".join(context_parts)

    def get_tactical_context(self, connection_alias: str) -> List[Dict[str, Any]]:
        """
        Builds a detailed, structured context for the ToolSpecialistAgent.
        It retrieves the full JSON Schema for all actions on a specific connection.

        Args:
            connection_alias: The alias of the connection to get tools for.

        Returns:
            A list of tool definitions in OpenAI Function Calling format.
        """
        if connection_alias not in self.state.connections:
            raise ValueError(f"Connection alias '{connection_alias}' is not active.")

        source = self.state.connections[connection_alias]
        try:
            # Load the full blueprint for the connection
            conn_model, _ = self.resolver.resolve(source)
            if not conn_model.catalog or not conn_model.catalog.browse_config:
                return []

            action_templates = conn_model.catalog.browse_config.get(
                "action_templates", {}
            )

            tools = []
            for action_name, config in action_templates.items():
                func_def = {
                    "name": f"{connection_alias}.{action_name}",
                    "description": config.get(
                        "description", f"Execute the {action_name} action."
                    ),
                    "parameters": {"type": "object", "properties": {}, "required": []},
                }

                model_name_str = config.get("parameters_model")
                if model_name_str and conn_model.catalog.schemas_module_path:
                    # Dynamically convert the Pydantic model to JSON Schema
                    schema = self._get_schema_for_model(
                        conn_model.catalog.schemas_module_path, model_name_str
                    )
                    if schema:
                        # Pydantic's model_json_schema includes a 'title' and 'description' we don't need at the top level.
                        # We extract the core properties and required fields.
                        func_def["parameters"]["properties"] = schema.get(
                            "properties", {}
                        )
                        func_def["parameters"]["required"] = schema.get("required", [])

                tools.append({"type": "function", "function": func_def})
            return tools

        except Exception:
            # Fail gracefully if blueprint loading fails
            return []

    def _get_schema_for_model(
        self, schemas_py_file: str, model_path_str: str
    ) -> Dict[str, Any] | None:
        """Dynamically loads a Pydantic model and converts it to a JSON Schema."""
        if not model_path_str.startswith("schemas."):
            return None

        class_name = model_path_str.split(".", 1)[1]
        try:
            spec = importlib.util.spec_from_file_location(
                f"blueprint_schemas_{Path(schemas_py_file).stem}", schemas_py_file
            )
            schemas_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(schemas_module)

            ParamModel = getattr(schemas_module, class_name)
            if issubclass(ParamModel, BaseModel):
                # Use Pydantic's built-in JSON schema generation
                return ParamModel.model_json_schema()
        except (FileNotFoundError, AttributeError, ImportError, Exception):
            # Catch all exceptions during dynamic loading to ensure stability.
            pass
        return None
