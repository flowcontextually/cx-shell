from typing import Any, Dict, List

import pandas as pd
import structlog
from jinja2 import ChoiceLoader, Environment, FileSystemLoader, select_autoescape
from pydantic import BaseModel, Field

from ..operations.file_format_ops import ArtifactType  # Import the shared ArtifactType
from ..vfs_client import AbstractVfsClient
from .base import BaseTransformEngine

logger = structlog.get_logger(__name__)


class RenderTemplateOp(BaseModel):
    """
    Defines the declarative operation for rendering a Jinja2 template.
    This model is used to validate the 'operation' block in a transformer script.
    """

    type: str = "render_template"
    template_path: str = Field(
        ...,
        description="Path to the Jinja2 template file (can be relative or absolute).",
    )
    target_path: str = Field(
        ..., description="Output path for the rendered artifact file."
    )
    artifact_type: ArtifactType = Field(
        "attachment",
        description="The semantic role of the rendered file (e.g., 'html_body', 'attachment').",
    )


class JinjaEngine(BaseTransformEngine):
    """
    A transformation engine that uses Jinja2 to render a template into a file.

    This engine is designed for creating presentation artifacts, such as HTML reports
    or email bodies, from a DataFrame and other summary data calculated in previous
    steps. It does not modify the DataFrame itself but produces a file as a
    side-effect and updates the run's "Artifact Manifest".
    """

    engine_name = "jinja"

    def __init__(self, vfs_client: AbstractVfsClient):
        """
        Initializes the JinjaEngine with a VFS client and a robust Jinja environment.

        Args:
            vfs_client: An instance of a VFS client for writing the output file.
        """
        self.vfs = vfs_client

        # This robust loader configuration handles both relative and absolute paths.
        # 1. FileSystemLoader('.'): Tries to find templates relative to the current working directory.
        # 2. FileSystemLoader('/'): Tries to find templates using an absolute path from the root.
        self.jinja_env = Environment(
            loader=ChoiceLoader([FileSystemLoader("."), FileSystemLoader("/")]),
            autoescape=select_autoescape(["html", "xml"]),  # Security best practice
        )

    async def transform(
        self,
        data: pd.DataFrame,
        operations: List[Dict[str, Any]],
        context: Dict[str, Any],
    ) -> pd.DataFrame:
        """
        Renders a Jinja2 template using the DataFrame and the run context,
        then saves the result to a file and updates the Artifact Manifest.

        Args:
            data: The input DataFrame, which will be made available to the template.
            operations: A list of declarative operations (expects one 'render_template' op).
            context: The shared run context, containing the 'artifacts' manifest
                     to be populated and any pre-calculated summary data.

        Returns:
            The original, unmodified DataFrame to be passed to the next step.
        """
        log = logger.bind(engine=self.engine_name)
        # This engine typically handles one operation per step.
        op_data = operations[0]
        op_model = RenderTemplateOp.model_validate(op_data)

        log.info(
            "Executing: render_template",
            template=op_model.template_path,
            target=op_model.target_path,
            artifact_type=op_model.artifact_type,
        )

        try:
            # Jinja will now correctly find the template using the ChoiceLoader.
            template = self.jinja_env.get_template(op_model.template_path)
        except Exception as e:
            log.error(
                "jinja.template_load_failed", path=op_model.template_path, error=str(e)
            )
            raise IOError(
                f"Failed to load Jinja2 template from '{op_model.template_path}': {e}"
            ) from e

        # Prepare the full context available to the template. This includes:
        # - The entire run context (e.g., context['report_summary'])
        # - The current DataFrame as a list of dictionaries (context['records'])
        # - Other useful metadata about the DataFrame.
        template_context = {
            **context,
            "records": data.to_dict("records"),
            "column_names": data.columns.tolist(),
            "record_count": len(data),
        }

        # Render the template with the combined context
        rendered_content = template.render(template_context)
        content_bytes = rendered_content.encode("utf-8")

        # Save the rendered content to the target file via the VFS client
        canonical_path = await self.vfs.write(
            path=op_model.target_path, content=content_bytes, context=context
        )

        # Populate the structured Artifact Manifest in the run context
        artifacts_manifest = context.get("artifacts", {})
        if op_model.artifact_type == "html_body":
            artifacts_manifest["html_body"] = canonical_path
        else:  # The default is 'attachment'.
            artifacts_manifest.setdefault("attachments", []).append(canonical_path)

        log.info(
            "template.render.success",
            path=canonical_path,
            bytes_written=len(content_bytes),
        )

        # This engine produces an artifact; it doesn't modify the DataFrame.
        # Return the original DataFrame for the next step in the pipeline.
        return data
