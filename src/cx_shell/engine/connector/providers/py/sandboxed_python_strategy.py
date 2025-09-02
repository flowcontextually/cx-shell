import json
from pathlib import Path
import subprocess
import sys
from contextlib import asynccontextmanager
from typing import Any, Dict, List, TYPE_CHECKING

import structlog

from ..base import BaseConnectorStrategy
from .....utils import get_asset_path
from cx_core_schemas.connection import Connection

if TYPE_CHECKING:
    from cx_core_schemas.vfs import VfsFileContentResponse

logger = structlog.get_logger(__name__)


class SandboxedPythonStrategy(BaseConnectorStrategy):
    """
    A strategy for executing Python scripts in an isolated environment.
    """

    strategy_key = "python-sandboxed"

    async def test_connection(
        self, connection: "Connection", secrets: Dict[str, Any]
    ) -> bool:
        return True

    @asynccontextmanager
    async def get_client(self, connection: "Connection", secrets: Dict[str, Any]):
        yield None

    async def run_python_script(
        self,
        connection: "Connection",
        action_params: Dict[str, Any],
        script_input: Dict[str, Any],  # <-- THIS IS THE FIX
    ) -> Dict[str, Any]:
        """
        Executes a specified Python script as a separate process.

        Args:
            connection: The connection model (unused for this strategy).
            action_params: The validated parameters from the RunPythonScriptAction.
            script_input: The context from the overall script run (unused by this action).
        """

        script_path_str = action_params["script_path"]

        if script_path_str.startswith("asset:"):
            relative_path = script_path_str.split(":", 1)[1]
            script_path = get_asset_path(relative_path)
        else:
            script_path = Path(script_path_str)

        input_data = action_params["input_data_json"]
        python_executable = sys.executable

        log = logger.bind(script_path=script_path, python_executable=python_executable)
        log.info("Executing sandboxed Python script.")

        try:
            process = subprocess.run(
                [python_executable, script_path],
                input=input_data,
                capture_output=True,
                text=True,
                check=True,
                timeout=120,
            )

            output_data = json.loads(process.stdout)
            log.info("Python script executed successfully.")
            return output_data

        except subprocess.CalledProcessError as e:
            log.error("Python script failed.", stderr=e.stderr.strip())
            raise IOError(f"Execution of script '{script_path}' failed: {e.stderr}")
        except json.JSONDecodeError as e:
            log.error(
                "Failed to parse JSON output from Python script.",
                error=str(e),
                stdout=process.stdout,
            )
            raise ValueError(f"Script '{script_path}' produced invalid JSON output.")
        except Exception as e:
            log.error(
                "An unexpected error occurred during Python script execution.",
                error=str(e),
            )
            raise

    # --- Fulfilling the Contract ---
    async def browse_path(
        self, path_parts: List[str], connection: "Connection", secrets: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        raise NotImplementedError

    async def get_content(
        self, path_parts: List[str], connection: "Connection", secrets: Dict[str, Any]
    ) -> "VfsFileContentResponse":
        raise NotImplementedError
