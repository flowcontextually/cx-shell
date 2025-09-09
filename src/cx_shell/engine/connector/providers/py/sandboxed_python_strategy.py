import json
from pathlib import Path
import subprocess
import sys
from contextlib import asynccontextmanager
from typing import Any, Dict, List, TYPE_CHECKING

import structlog

from ..base import BaseConnectorStrategy
from .....utils import get_assets_root
from .....data.agent_schemas import DryRunResult
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
        self, connection: Connection, secrets: Dict[str, Any]
    ) -> bool:
        return True

    @asynccontextmanager
    async def get_client(self, connection: Connection, secrets: Dict[str, Any]):
        yield None

    async def run_python_script(
        self,
        connection: Connection,
        action_params: Dict[str, Any],
        script_input: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Executes a specified Python script as a separate process, correctly
        resolving paths for both system assets and user-provided scripts.
        """
        script_path_str = action_params["script_path"]

        script_path_obj: Path
        if script_path_str.startswith("asset:"):
            relative_path = script_path_str.split(":", 1)[1]
            # Get the assets root and build the path from there.
            assets_root = get_assets_root()
            script_path_obj = assets_root / relative_path
        else:
            script_path_obj = Path(script_path_str).expanduser().resolve()

        final_script_path = str(script_path_obj)

        input_data = action_params["input_data_json"]
        python_executable = sys.executable

        log = logger.bind(
            script_path=final_script_path, python_executable=python_executable
        )
        log.info("Executing sandboxed Python script.")

        try:
            process = subprocess.run(
                [python_executable, final_script_path],
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
            raise IOError(
                f"Execution of script '{final_script_path}' failed: {e.stderr}"
            )
        except json.JSONDecodeError as e:
            log.error(
                "Failed to parse JSON output from Python script.",
                error=str(e),
                stdout=process.stdout,
            )
            raise ValueError(
                f"Script '{final_script_path}' produced invalid JSON output."
            )
        except Exception as e:
            log.error(
                "An unexpected error occurred during Python script execution.",
                error=str(e),
            )
            raise

    async def browse_path(
        self, path_parts: List[str], connection: Connection, secrets: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        raise NotImplementedError

    async def get_content(
        self, path_parts: List[str], connection: Connection, secrets: Dict[str, Any]
    ) -> "VfsFileContentResponse":
        raise NotImplementedError

    async def dry_run(
        self,
        connection: Connection,
        secrets: Dict[str, Any],
        action_params: Dict[str, Any],
    ) -> "DryRunResult":
        return DryRunResult(
            indicates_failure=False,
            message="Dry run successful by default for this strategy.",
        )
