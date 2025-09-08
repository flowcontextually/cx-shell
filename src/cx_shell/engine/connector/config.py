import os
import re
import json
import zipfile
import io
import shutil
from pathlib import Path
from typing import Any, Dict, Tuple

import structlog
import yaml
import httpx
from dotenv import dotenv_values
from pydantic import ValidationError

from cx_core_schemas.connection import Connection
from cx_core_schemas.api_catalog import ApiCatalog

logger = structlog.get_logger(__name__)

# --- Centralized Path and URI Constants ---
CX_HOME = Path(os.getenv("CX_HOME", Path.home() / ".cx"))
BLUEPRINTS_BASE_PATH = Path(os.getenv("CX_BLUEPRINTS_PATH", CX_HOME / "blueprints"))
BLUEPRINTS_GITHUB_ORG = "flowcontextually"
BLUEPRINTS_GITHUB_REPO = "blueprints"


class ConnectionResolver:
    """
    Abstracts away the source of connection details and secrets. It also
    handles the on-demand downloading and caching of blueprint packages from
    the central GitHub repository's releases.
    """

    blueprint_regex = re.compile(
        r"^(?P<namespace>[\w-]+)/(?P<name>[\w-]+)@(?P<version>[\w\.-]+)$"
    )

    def __init__(self, db_client: Any = None, vault_client: Any = None):
        self.db = db_client
        self.vault = vault_client
        self.is_standalone = not (db_client and vault_client)
        self.user_connections_dir = CX_HOME / "connections"
        self.user_secrets_dir = CX_HOME / "secrets"
        logger.info(
            "ConnectionResolver initialized.", blueprints_path=str(BLUEPRINTS_BASE_PATH)
        )

    def _ensure_blueprint_exists_locally(self, blueprint_match: re.Match):
        parts = blueprint_match.groupdict()
        namespace, name, version_from_id = (
            parts["namespace"],
            parts["name"],
            parts["version"],
        )

        # Canonical local version NEVER has a 'v' prefix.
        version = version_from_id.lstrip("v")
        local_path = BLUEPRINTS_BASE_PATH / namespace / name / version

        if local_path.is_dir() and any(local_path.iterdir()):
            logger.debug(
                "Blueprint package found in local cache.", path=str(local_path)
            )
            return

        # Tag name for the URL MUST have a 'v' prefix.
        tag_version = f"v{version}"
        tag_name = f"{namespace}-{name}-{tag_version}"
        asset_name = f"{name}.zip"
        asset_url = f"https://github.com/{BLUEPRINTS_GITHUB_ORG}/{BLUEPRINTS_GITHUB_REPO}/releases/download/{tag_name}/{asset_name}"

        try:
            with httpx.stream(
                "GET", asset_url, follow_redirects=True, timeout=30.0
            ) as response:
                response.raise_for_status()
                zip_content = io.BytesIO(response.read())

            local_path.mkdir(parents=True, exist_ok=True)

            with zipfile.ZipFile(zip_content) as zf:
                for member in zf.infolist():
                    # This logic correctly handles nested directories inside the zip
                    path_parts = Path(member.filename).parts
                    target_filename = (
                        Path(*path_parts[1:])
                        if len(path_parts) > 1
                        else Path(member.filename)
                    )
                    target_path = local_path / target_filename
                    if not member.is_dir():
                        target_path.parent.mkdir(parents=True, exist_ok=True)
                        with open(target_path, "wb") as f:
                            f.write(zf.read(member))

            logger.info(
                "Successfully downloaded and extracted blueprint.", path=str(local_path)
            )
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                raise FileNotFoundError(
                    f"Blueprint '{blueprint_match.string}' not found. No corresponding release asset exists at {asset_url}"
                ) from e
            else:
                raise IOError(
                    f"Failed to download blueprint. HTTP error: {e.response.status_code}"
                ) from e
        except Exception as e:
            if local_path.exists():
                shutil.rmtree(local_path)
            raise IOError(
                f"Failed to download or extract blueprint '{blueprint_match.string}'. Error: {e}"
            ) from e

    def load_blueprint_by_id(self, blueprint_id: str) -> ApiCatalog:
        """Public method to ensure existence and then load a blueprint by its ID."""
        log = logger.bind(blueprint_id=blueprint_id)
        log.info("Attempting to load blueprint by ID.")

        blueprint_match = self.blueprint_regex.match(blueprint_id)
        if not blueprint_match:
            raise ValueError(
                f"'{blueprint_id}' is not a valid blueprint ID format (e.g., 'namespace/name@version')."
            )

        # 1. Ensure the blueprint exists locally, downloading if necessary.
        self._ensure_blueprint_exists_locally(blueprint_match)

        # 2. Load the package from the now-populated local cache.
        blueprint_data = self._load_blueprint_package(blueprint_match)
        return ApiCatalog(**blueprint_data)

    async def resolve(self, source: str) -> Tuple[Connection, Dict[str, Any]]:
        """
        Resolves a connection source string into a fully hydrated Connection
        Pydantic model and a dictionary of its secrets.
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
        Handles loading a connection from a .yaml file, triggering a blueprint
        sync if necessary, and loading its secrets.
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

            if self.blueprint_regex.match(connection_model.api_catalog_id or ""):
                try:
                    # The high-level resolve now calls the public load method, which handles syncing.
                    blueprint_catalog = self.load_blueprint_by_id(
                        connection_model.api_catalog_id
                    )
                    connection_model.catalog = blueprint_catalog
                    log.info(
                        "Successfully loaded and merged blueprint package.",
                        blueprint=connection_model.api_catalog_id,
                    )
                except (FileNotFoundError, ValidationError, ValueError) as e:
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
            secrets = {
                k.lower(): v
                for k, v in dotenv_values(dotenv_path=secrets_file).items()
                if v is not None
            }

        return connection_model, secrets

    def _load_blueprint_package(self, blueprint_match: re.Match) -> Dict[str, Any]:
        """Loads all artifacts from a blueprint package directory in the local cache."""
        parts = blueprint_match.groupdict()

        # --- THIS IS THE FIX ---
        # The version string from the regex might contain a 'v' (e.g., "v0.1.1").
        # We MUST strip it here to match the canonical directory path, which is
        # always stored without the 'v' (e.g., ".../mssql/0.1.1/").
        version = parts["version"].lstrip("v")
        blueprint_dir = (
            BLUEPRINTS_BASE_PATH / parts["namespace"] / parts["name"] / version
        )
        # --- END FIX ---

        blueprint_path = blueprint_dir / "blueprint.cx.yaml"
        source_spec_path = blueprint_dir / "source_spec.json"
        schemas_py_path = blueprint_dir / "schemas.py"

        logger.debug(
            "Attempting to load blueprint package from local cache.",
            path=str(blueprint_dir),
        )

        if not blueprint_path.is_file():
            raise FileNotFoundError(
                f"Blueprint file 'blueprint.cx.yaml' not found at: {blueprint_dir}"
            )

        with open(blueprint_path, "r", encoding="utf-8") as f:
            blueprint_data = yaml.safe_load(f)

        if source_spec_path.is_file():
            with open(source_spec_path, "r", encoding="utf-8") as f:
                blueprint_data["source_spec"] = json.load(f)

        if schemas_py_path.is_file():
            blueprint_data["schemas_module_path"] = str(schemas_py_path)

        return blueprint_data

    async def _resolve_from_db(self, connection_id: str):
        raise NotImplementedError("_resolve_from_db is not yet implemented.")
