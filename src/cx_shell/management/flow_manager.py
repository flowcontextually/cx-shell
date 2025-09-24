from pathlib import Path
import yaml
from typing import List, Dict, Any, Tuple

import structlog
from ..engine.connector.service import ConnectorService
from ..interactive.session import SessionState
from .workspace_manager import WorkspaceManager  # <-- NEW IMPORT

logger = structlog.get_logger(__name__)


class FlowManager:
    """Handles logic for listing and running .flow.yaml files from the multi-rooted workspace."""

    def __init__(self):
        """Initializes the manager with the new WorkspaceManager."""
        self.workspace_manager = WorkspaceManager()

    def _get_search_paths(self) -> List[Tuple[str, Path]]:
        """
        Defines the prioritized search paths for flows by querying the WorkspaceManager.
        Returns a list of (namespace, path_object) tuples.
        """
        # --- DEFINITIVE FIX: Use WorkspaceManager to get roots ---
        search_paths = []
        all_roots = self.workspace_manager.get_roots()

        for root_path in all_roots:
            namespace = "system" if ".cx" in str(root_path) else root_path.name
            flow_dir = root_path / "flows"
            search_paths.append((namespace, flow_dir))

        # We no longer need to manually add the CWD. The user should explicitly add it via `cx workspace add .`
        return search_paths
        # --- END FIX ---

    def list_flows(self) -> List[Dict[str, str]]:
        """
        Lists all available flows from all registered workspace roots.
        """
        flows_data = []
        found_names = set()

        for namespace, search_path in self._get_search_paths():
            if not search_path.is_dir():
                continue

            for flow_file in sorted(search_path.glob("*.flow.yaml")):
                flow_name = flow_file.stem.replace(".flow", "")

                # Create a unique namespaced ID to prevent collisions
                namespaced_id = f"{namespace}/{flow_name}"
                if namespaced_id in found_names:
                    continue
                found_names.add(namespaced_id)

                description = "No description."
                try:
                    with open(flow_file, "r") as f:
                        data = yaml.safe_load(f)
                        description = data.get("description", "No description.")
                except Exception:
                    description = "[red]Error reading file[/red]"

                flows_data.append(
                    {
                        "Name": namespaced_id,
                        "Description": description,
                        "Source": namespace,
                    }
                )

        return flows_data

    def _find_flow(self, name: str) -> Path:
        """
        Finds a flow by its potentially namespaced name across all workspace roots.
        """
        # If the name is already namespaced (e.g., "acme-ops/my-flow")
        # name = "system/cxcontext"
        if "/" in name:
            namespace, flow_name = name.split("/", 1)
            for ns, search_path in self._get_search_paths():
                if ns == namespace:
                    flow_path = search_path / f"{flow_name}.flow.yaml"
                    if flow_path.exists():
                        return flow_path
        else:
            # If no namespace, search in all roots
            for _, search_path in self._get_search_paths():
                flow_path = search_path / f"{name}.flow.yaml"
                if flow_path.exists():
                    return flow_path

        raise FileNotFoundError(
            f"Flow '{name}' not found in any registered workspace root."
        )

    async def run_flow(
        self,
        state: SessionState,
        service: ConnectorService,
        named_args: Dict[str, Any],
    ) -> Any:
        """Executes a flow by name, finding it in the multi-rooted workspace."""
        logger.debug("flow_manager.run_flow.received", named_args=named_args)

        name = named_args.pop("name", None)
        no_cache = named_args.pop("no-cache", False)  # <-- ADD THIS LINE
        params = named_args.get("params", {})

        if not name:
            raise ValueError("`flow run` requires a '--name <flow_name>' argument.")

        flow_path = self._find_flow(name)
        logger.info("flow_manager.run_flow.resolved", flow_path=str(flow_path))

        return await service.engine.run_script(
            script_path=flow_path,
            script_input=params,
            session_variables=state.variables,
            no_cache=no_cache,
        )
