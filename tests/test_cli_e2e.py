# In tests/test_cli_e2e.py

from pathlib import Path
import yaml
import pytest
from pytest_mock import MockerFixture

from cx_shell.engine.connector.service import ConnectorService
from cx_shell.engine.connector import config as connector_config
from cx_shell.cli import init as cx_init_func


# --- REFACTORED FIXTURE ---
@pytest.fixture
def clean_cx_home(tmp_path, monkeypatch):
    """
    Creates a pristine, isolated ~/.cx home for each test and redirects the app to use it.
    This removes dependency on the user's real ~/.cx and ensures test isolation.
    """
    temp_cx_home = tmp_path / ".cx"
    monkeypatch.setattr(connector_config, "CX_HOME", temp_cx_home)
    # Also patch the BLUEPRINTS_BASE_PATH to be inside our temp home
    monkeypatch.setattr(
        connector_config, "BLUEPRINTS_BASE_PATH", temp_cx_home / "blueprints"
    )
    yield temp_cx_home
    # No teardown needed, tmp_path handles it.


def test_init_command_succeeds_locally(clean_cx_home: Path):
    """
    Tests that the `cx init` function correctly creates the expected files
    and directories in a clean environment, without relying on external state.
    """
    # Run the init command programmatically, not as a subprocess
    cx_init_func()

    # Assert that the core directories are created
    assert (clean_cx_home / "connections").is_dir()
    assert (clean_cx_home / "blueprints" / "community").is_dir()

    # Assert that the bundled GitHub blueprint was copied correctly
    # --- THIS IS THE FIX ---
    # The version in the YAML is "0.1.0", so the created directory
    # will not have the 'v' prefix. The test must match this reality.
    github_blueprint_path = (
        clean_cx_home / "blueprints" / "community" / "github" / "0.1.0"
    )
    # --- END FIX ---
    assert (github_blueprint_path / "blueprint.cx.yaml").is_file()
    assert (github_blueprint_path / "schemas.py").is_file()


# --- REFACTORED TEST 2 ---
@pytest.mark.asyncio
async def test_blueprint_action_with_mocked_api(
    clean_cx_home: Path, mocker: MockerFixture
):
    """
    Tests the end-to-end blueprint execution flow by mocking the HTTP request
    and manually creating the required configuration in an isolated environment.
    """
    # 1. Manually create the necessary blueprint and connection files in our clean env.
    # This completely removes the need for `cx init` or network downloads.
    blueprint_dir = clean_cx_home / "blueprints" / "community" / "github" / "v0.1.0"
    blueprint_dir.mkdir(parents=True)

    # Simplified blueprint content for the test
    (blueprint_dir / "blueprint.cx.yaml").write_text(
        yaml.dump(
            {
                "id": "blueprint:community-github-starter",
                "name": "GitHub API (Starter)",
                "version": "0.1.0",
                "connector_provider_key": "rest-declarative",
                "browse_config": {
                    "base_url_template": "https://api.github.com",
                    "action_templates": {
                        "getUser": {
                            "http_method": "GET",
                            "api_endpoint": "/users/{{ context.username }}",
                        }
                    },
                },
            }
        )
    )

    connection_dir = clean_cx_home / "connections"
    connection_dir.mkdir()
    (connection_dir / "github.conn.yaml").write_text(
        yaml.dump(
            {
                "name": "GitHub Public API",
                "id": "user:github",
                "api_catalog_id": "community/github@v0.1.0",
                "auth_method_type": "none",
            }
        )
    )

    # 2. Mock the `httpx.AsyncClient.request` method (this part is the same)
    mock_user_data = {"login": "torvalds", "id": 1024025, "name": "Linus Torvalds"}
    mock_response = mocker.Mock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = mock_user_data
    mocker.patch("httpx.AsyncClient.request", return_value=mock_response)

    # 3. Create the workflow script (this part is the same)
    script_content = {
        "name": "E2E Test Script for GitHub",
        "steps": [
            {
                "id": "get_github_user",
                "name": "Get GitHub User",
                "connection_source": "user:github",
                "run": {
                    "action": "run_declarative_action",
                    "template_key": "getUser",
                    "context": {"username": "torvalds"},
                },
            }
        ],
    }
    script_file = clean_cx_home / "test_github.connector.yaml"
    script_file.write_text(yaml.dump(script_content))

    # 4. Execute the script in-process.
    service = ConnectorService()
    results = await service.run_script(script_file)

    # 5. Assert that the result is no longer an error object.
    assert "Get GitHub User" in results
    user_data = results["Get GitHub User"]
    assert isinstance(user_data, dict)
    assert "error" not in user_data
    assert user_data["id"] == 1024025
    assert user_data["login"] == "torvalds"
