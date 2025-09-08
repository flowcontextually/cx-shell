from pathlib import Path
import pytest
from cx_shell.engine.connector.config import ConnectionResolver
from cx_core_schemas.api_catalog import ApiCatalog


def test_init_command_succeeds_locally(clean_cx_home: Path):
    """
    Unit Test: Verifies the `cx init` command correctly populates a clean
    workspace directory without external dependencies.
    """
    from cx_shell.cli import init as cx_init_func

    # The clean_cx_home fixture ensures this runs in an isolated directory.
    cx_init_func()

    assert (clean_cx_home / "connections").is_dir()
    assert (clean_cx_home / "blueprints" / "community" / "github" / "0.1.0").is_dir()


@pytest.mark.network
def test_resolver_on_demand_blueprint_download(clean_cx_home: Path):
    """
    Integration Test: Verifies the ConnectionResolver can successfully
    download and cache a real blueprint from GitHub Releases.
    Requires a live internet connection.
    """
    resolver = ConnectionResolver()

    # Use a real, stable blueprint from the public registry
    blueprint_id_to_test = "community/sendgrid@0.3.0"

    # Act: This will trigger the download and cache mechanism.
    catalog = resolver.load_blueprint_by_id(blueprint_id_to_test)

    # Assert: Check that the blueprint was loaded and files exist.
    assert isinstance(catalog, ApiCatalog)
    assert catalog.name == "SendGrid API"

    expected_path = clean_cx_home / "blueprints" / "community" / "sendgrid" / "0.3.0"
    assert (expected_path / "blueprint.cx.yaml").is_file()
    assert (expected_path / "schemas.py").is_file()
