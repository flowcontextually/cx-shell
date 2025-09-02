import os
import re
import json
from pathlib import Path
from typing import Any, Dict, Tuple

import structlog
import yaml
from dotenv import dotenv_values
from pydantic import ValidationError

from cx_core_schemas.connection import Connection
from cx_core_schemas.api_catalog import ApiCatalog

logger = structlog.get_logger(__name__)

# --- Blueprint Configuration ---
BLUEPRINTS_BASE_PATH = Path(
    os.getenv("CX_BLUEPRINTS_PATH", Path.home() / ".cx" / "blueprints")
)


class ConnectionResolver:
    """
    Abstracts away the source of connection details and secrets. It also
    handles finding, loading, and merging the correct versioned blueprint from
    the filesystem, making connections "executable".
    """

    def __init__(self, db_client: Any = None, vault_client: Any = None):
        self.db = db_client
        self.vault = vault_client
        self.is_standalone = not (db_client and vault_client)
        self.user_connections_dir = Path.home() / ".cx" / "connections"
        self.user_secrets_dir = Path.home() / ".cx" / "secrets"
        log = logger.bind(
            mode="standalone" if self.is_standalone else "integrated",
            blueprints_path=str(BLUEPRINTS_BASE_PATH),
        )
        log.info("ConnectionResolver initialized.")

    async def resolve(self, source: str) -> Tuple[Connection, Dict[str, Any]]:
        """
        Resolves a connection source string into a fully hydrated Connection
        Pydantic model (with its blueprint loaded) and a dictionary of its secrets.
        """
        log = logger.bind(source=source)
        log.info("Resolving connection source.")

        if source.startswith("db:"):
            raise ValueError(
                "Database source 'db:' is not available in standalone mode."
            )

        elif source.startswith("user:"):
            conn_name = source.split(":", 1)[1]
            conn_path = self.user_connections_dir / f"{conn_name}.conn.yaml"
            if not conn_path.exists():
                raise FileNotFoundError(
                    f"User connection '{conn_name}' not found at: {conn_path}"
                )
            return self._resolve_from_file(conn_path)

        elif source.startswith("file:"):
            path_str = source.split(":", 1)[1]
            return self._resolve_from_file(Path(path_str))
        else:
            raise ValueError(f"Unknown connection source protocol: {source}")

    def _resolve_from_file(
        self, conn_file: Path, secrets_file: Path = None
    ) -> Tuple["Connection", Dict[str, Any]]:
        """
        Handles loading a connection from a .yaml file, loading its corresponding
        blueprint (if applicable), and loading its secrets.
        """
        log = logger.bind(path=str(conn_file))
        if not conn_file.is_file():
            raise FileNotFoundError(
                f"Connection configuration file not found: {conn_file}"
            )

        with open(conn_file, "r", encoding="utf-8") as f:
            raw_data = yaml.safe_load(f)

        try:
            if "id" not in raw_data:
                raw_data["id"] = f"user:{conn_file.stem.replace('.conn', '')}"

            connection_model = Connection(**raw_data)

            blueprint_match = re.match(
                r"^(?P<namespace>[\w-]+)/(?P<name>[\w-]+)@(?P<version>[\w\.-]+)$",
                connection_model.api_catalog_id or "",
            )

            if blueprint_match:
                log.info(
                    "Blueprint-style api_catalog_id detected. Loading blueprint.",
                    catalog_id=connection_model.api_catalog_id,
                )
                try:
                    blueprint_data = self._load_blueprint_package(blueprint_match)
                    blueprint_catalog = ApiCatalog(**blueprint_data)
                    connection_model.catalog = blueprint_catalog
                    log.info(
                        "Successfully loaded and merged blueprint package.",
                        blueprint=connection_model.api_catalog_id,
                    )
                except (FileNotFoundError, ValidationError) as e:
                    log.error(
                        "Failed to load blueprint, connection may have limited capabilities.",
                        blueprint=connection_model.api_catalog_id,
                        error=str(e),
                    )
            else:
                log.debug(
                    "No blueprint-style api_catalog_id found. Using catalog data from connection file.",
                    catalog_id=connection_model.api_catalog_id,
                )

        except ValidationError as e:
            raise ValueError(f"Invalid schema in '{conn_file.name}': {e}") from e

        secrets: Dict[str, Any] = {}
        if not secrets_file:
            secrets_file = (
                self.user_secrets_dir
                / f"{conn_file.stem.replace('.conn', '')}.secret.env"
            )

        if secrets_file.exists():
            raw_secrets = dotenv_values(dotenv_path=secrets_file)
            secrets = {k.lower(): v for k, v in raw_secrets.items() if v is not None}

        return connection_model, secrets

    def _load_blueprint_package(self, blueprint_match: re.Match) -> Dict[str, Any]:
        """
        Loads all artifacts from a blueprint package directory (blueprint.cx.yaml,
        source_spec.json, and the path to schemas.py).

        Args:
            blueprint_match: A regex match object containing the namespace, name, and version.

        Returns:
            A dictionary containing the contents of the entire blueprint package,
            ready to be validated by the ApiCatalog model.
        """
        parts = blueprint_match.groupdict()
        blueprint_dir = (
            BLUEPRINTS_BASE_PATH / parts["namespace"] / parts["name"] / parts["version"]
        )

        blueprint_path = blueprint_dir / "blueprint.cx.yaml"
        source_spec_path = blueprint_dir / "source_spec.json"
        schemas_py_path = blueprint_dir / "schemas.py"

        logger.debug("Attempting to load blueprint package.", path=str(blueprint_dir))

        if not blueprint_path.is_file():
            raise FileNotFoundError(
                f"Blueprint file 'blueprint.cx.yaml' not found at: {blueprint_dir}"
            )

        with open(blueprint_path, "r", encoding="utf-8") as f:
            blueprint_data = yaml.safe_load(f)

        if source_spec_path.is_file():
            with open(source_spec_path, "r", encoding="utf-8") as f:
                blueprint_data["source_spec"] = json.load(f)
        else:
            logger.warning(
                "source_spec.json not found for blueprint; parameter validation will be disabled.",
                path=str(blueprint_dir),
            )

        if schemas_py_path.is_file():
            blueprint_data["schemas_module_path"] = str(schemas_py_path)
        else:
            logger.warning(
                "schemas.py not found for blueprint; parameter validation will be disabled.",
                path=str(blueprint_dir),
            )

        return blueprint_data

    async def _resolve_from_db(self, connection_id: str):
        """Handles fetching from the live platform (DB + Vault)."""
        raise NotImplementedError("_resolve_from_db is not yet implemented.")
