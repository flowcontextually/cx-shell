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


@pytest.fixture
def clean_cx_home(tmp_path: Path, monkeypatch):
    """
    A project-wide fixture that creates a pristine, isolated ~/.cx home for
    each test and redirects all parts of the application to use it.

    This is the cornerstone of our test isolation strategy.
    """
    temp_cx_home = tmp_path / ".cx"

    # Patch all known modules that reference the CX_HOME constant.
    monkeypatch.setattr(connector_config, "CX_HOME", temp_cx_home)
    monkeypatch.setattr(
        connector_config, "BLUEPRINTS_BASE_PATH", temp_cx_home / "blueprints"
    )
    monkeypatch.setattr(session_manager, "CX_HOME", temp_cx_home)
    monkeypatch.setattr(flow_manager, "CX_HOME", temp_cx_home)
    monkeypatch.setattr(query_manager, "CX_HOME", temp_cx_home)
    monkeypatch.setattr(script_manager, "CX_HOME", temp_cx_home)
    monkeypatch.setattr(connection_manager, "CX_HOME", temp_cx_home)
    monkeypatch.setattr(app_manager, "CX_HOME", temp_cx_home)
    monkeypatch.setattr(process_manager, "CX_HOME", temp_cx_home)
    monkeypatch.setattr(history_logger, "CX_HOME", temp_cx_home)

    # The HistoryLogger also creates a subdirectory we need to patch.
    monkeypatch.setattr(history_logger, "CONTEXT_DIR", temp_cx_home / "context")

    yield temp_cx_home
