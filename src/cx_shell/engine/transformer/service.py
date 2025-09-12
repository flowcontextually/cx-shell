import json
from typing import Any, Dict, List

import pandas as pd
import structlog

from .engines.base import BaseTransformEngine
from .engines.file_format_engine import FileFormatEngine
from .engines.jinja_engine import JinjaEngine
from .engines.pandas_engine import PandasEngine
from .vfs_client import AbstractVfsClient, LocalVfsClient

logger = structlog.get_logger(__name__)


class TransformerService:
    """
    Orchestrates a multi-step data transformation pipeline.

    This service loads raw data, passes it sequentially through a series of
    declarative "engine" steps (e.g., Pandas, Jinja), and produces a final
    output. The output is either a structured "Artifact Manifest" detailing the
    files created, or the in-memory transformed data if no files were saved.
    """

    def __init__(self, vfs_client: AbstractVfsClient | None = None):
        """
        Initializes the service and registers all available transformation engines.

        Args:
            vfs_client: An optional VFS client for handling file I/O. If not
                        provided, it defaults to a client for the local filesystem.
        """
        if vfs_client is None:
            vfs_client = LocalVfsClient()
        self.vfs_client = vfs_client
        self.engines: Dict[str, BaseTransformEngine] = {
            "pandas": PandasEngine(),
            "file_format": FileFormatEngine(self.vfs_client),
            "jinja": JinjaEngine(self.vfs_client),
        }
        logger.info(
            "TransformerService initialized.",
            registered_engines=list(self.engines.keys()),
        )

    async def run(self, script_data: Dict, run_context: Dict) -> Any:
        """
        Executes a full transformation pipeline, intelligently unpacking input data.
        """
        log = logger.bind(script_name=script_data.get("name"))
        log.info("service.run.begin")

        initial_input = run_context.get("initial_input")
        # Allow empty lists as valid input, but raise error for None/missing input.
        if initial_input is None:
            raise ValueError(
                "Transformer service received no 'initial_input' in its context."
            )

        # Intelligently unpack the data from common wrapper formats.
        unpacked_data = initial_input

        # Check for the transformer's own output format: {"results": [...]}
        if isinstance(unpacked_data, dict) and "results" in unpacked_data:
            unpacked_data = unpacked_data["results"]

        # Check for the VfsFileContentResponse format: {"content": "[...]", ...}
        if isinstance(unpacked_data, dict) and "content" in unpacked_data:
            try:
                # The content is a JSON string, so we must parse it.
                unpacked_data = json.loads(unpacked_data["content"])
            except (json.JSONDecodeError, TypeError):
                log.warn(
                    "Could not parse 'content' field as JSON, proceeding with raw value."
                )
                pass  # Keep unpacked_data as is if content isn't valid JSON

        # Get query_parameters from the run_context first, defaulting to {} if not present.
        query_parameters = run_context.get("query_parameters", {})

        list_of_records = []

        if isinstance(unpacked_data, list):
            list_of_records = unpacked_data
        elif isinstance(unpacked_data, dict):
            # This check is now redundant given our new flow, but safe to keep.
            list_of_records = [unpacked_data]
        else:
            raise TypeError(
                f"Unsupported input type for transformer: {type(unpacked_data).__name__}"
            )

        log.info(
            "Extracted data from pipeline input.",
            record_count=len(list_of_records),
            parameters=query_parameters,
        )

        run_context["query_parameters"] = query_parameters
        artifacts_manifest = {"attachments": []}
        run_context["artifacts"] = artifacts_manifest

        current_df = pd.DataFrame(list_of_records)
        if current_df.empty and list_of_records:
            current_df = pd.DataFrame(list_of_records, columns=["value"])

        log.info("service.run.loaded_initial_data", shape=current_df.shape)

        for i, step in enumerate(script_data.get("steps", [])):
            # ... (rest of the method is the same) ...
            engine_name = step.get("engine")
            engine = self.engines.get(engine_name)
            if not engine:
                raise ValueError(f"Unknown transformer engine: '{engine_name}'")
            log.info(
                "service.run.executing_step",
                step_index=i,
                step_name=step.get("name"),
                engine=engine_name,
            )
            operations = step.get("operations", [])
            if not operations and "operation" in step:
                operations = [step["operation"]]
            current_df = await engine.transform(
                data=current_df, operations=operations, context=run_context
            )

        log.info("service.run.finished", final_shape=current_df.shape)
        final_output = {"query_parameters": query_parameters}

        if artifacts_manifest.get("html_body") or artifacts_manifest["attachments"]:
            final_output["artifacts"] = artifacts_manifest
        else:
            final_output["results"] = current_df.to_dict("records")

        return final_output

    def _get_data_from_input(self, input_data: Any) -> List[Dict]:
        """
        Extracts the list of records from the connector's typical output
        format, which is {"step_name": [...]}. This makes the pipeline robust.
        """
        if isinstance(input_data, list):
            return input_data
        if isinstance(input_data, dict):
            # Find the first value in the dictionary that is a list.
            for value in input_data.values():
                if isinstance(value, list):
                    return value
        raise ValueError("Could not find a list of records in the input JSON data.")
