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
        Executes a full transformation pipeline defined by a script.

        This method orchestrates the entire process:
        1. Extracts the raw data and any accompanying parameters from the input.
        2. Places parameters into the shared run context for use by engines like Jinja.
        3. Sequentially executes each transformation step.
        4. Constructs a final output manifest containing both the generated artifact
           paths and the original query parameters for full traceability.

        Args:
            script_data: The parsed dictionary from a .transformer.yaml script.
            run_context: A dictionary containing the initial input data and for
                         sharing state between transformation steps.

        Returns:
            A dictionary containing the final output, which will be either an
            Artifact Manifest (if files were created) or the transformed data,
            always including the original query parameters.
        """

        log = logger.bind(script_name=script_data.get("name"))
        log.info("service.run.begin")

        initial_input = run_context.get("initial_input")
        if not initial_input or not isinstance(initial_input, dict):
            raise ValueError(
                "Input from connector-logic must be a non-empty dictionary."
            )

        # The input from the first stage is now structured: {"StepName": {"parameters": ..., "data": [...]}}
        # We find the first step's result in the dictionary to extract its contents.
        first_step_result = next(iter(initial_input.values()), {})

        # Extract the raw data rows and the parameters that generated them.
        list_of_records = first_step_result.get("data", [])
        query_parameters = first_step_result.get("parameters", {})

        log.info(
            "Extracted data from pipeline input.",
            record_count=len(list_of_records),
            parameters=query_parameters,
        )

        # Initialize the run context with the captured parameters.
        # This makes {{ query_parameters.start_date }} available to all engines.
        run_context["query_parameters"] = query_parameters

        # Initialize the Artifact Manifest. This will be populated by the file-saving engines.
        artifacts_manifest = {"attachments": []}
        run_context["artifacts"] = artifacts_manifest

        # Load the raw data into a DataFrame, which is our internal standard.
        current_df = pd.DataFrame(list_of_records)
        log.info("service.run.loaded_initial_data", shape=current_df.shape)

        # Sequentially execute each transformation step defined in the script.
        for i, step in enumerate(script_data.get("steps", [])):
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

            # Pass the DataFrame and the full context to the engine for processing.
            current_df = await engine.transform(
                data=current_df, operations=operations, context=run_context
            )

        log.info("service.run.finished", final_shape=current_df.shape)

        # Construct the final output object, always including the query parameters.
        final_output = {"query_parameters": query_parameters}

        if artifacts_manifest.get("html_body") or artifacts_manifest["attachments"]:
            final_output["artifacts"] = artifacts_manifest
            log.info(
                "Artifacts were generated, returning the full manifest.",
                manifest=final_output,
            )
        else:
            final_output["results"] = current_df.to_dict("records")
            log.info("No artifacts generated, returning transformed data.")

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
