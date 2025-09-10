from pathlib import Path
import pytest

# Import all modules that rely on CX_HOME so we can patch them all in one place.
from cx_shell.engine.connector import config as connector_config
from cx_shell.management import (
    session_manager,
    flow_manager,
    query_manager,
    script_manager,
    connection_manager,
    app_manager,
    process_manager,
)
from cx_shell import history_logger
from cx_shell import utils


@pytest.fixture
def clean_cx_home(tmp_path: Path, monkeypatch):
    """
    A project-wide fixture that creates a pristine, isolated ~/.cx home for
    each test and redirects all parts of the application to use it.
    """
    temp_cx_home = tmp_path / ".cx"

    # --- THIS IS THE FIX ---
    # We now patch the constants in their correct, final locations.
    monkeypatch.setattr(utils, "CX_HOME", temp_cx_home)
    monkeypatch.setattr(
        connector_config, "BLUEPRINTS_BASE_PATH", temp_cx_home / "blueprints"
    )
    # --- END FIX ---

    monkeypatch.setattr(session_manager, "CX_HOME", temp_cx_home)
    monkeypatch.setattr(flow_manager, "CX_HOME", temp_cx_home)
    monkeypatch.setattr(query_manager, "CX_HOME", temp_cx_home)
    monkeypatch.setattr(script_manager, "CX_HOME", temp_cx_home)
    monkeypatch.setattr(connection_manager, "CX_HOME", temp_cx_home)
    monkeypatch.setattr(app_manager, "CX_HOME", temp_cx_home)
    monkeypatch.setattr(process_manager, "CX_HOME", temp_cx_home)
    monkeypatch.setattr(history_logger, "CX_HOME", temp_cx_home)
    monkeypatch.setattr(history_logger, "CONTEXT_DIR", temp_cx_home / "context")

    yield temp_cx_home
