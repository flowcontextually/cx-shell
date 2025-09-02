import subprocess
import shutil
from pathlib import Path
import json

import pytest
import yaml
from pytest_mock import MockerFixture

# We import the application's core service to test it directly.
from cx_shell.engine.connector.service import ConnectorService

CX_HOME = Path.home() / ".cx"


@pytest.fixture(scope="module", autouse=True)
def setup_environment():
    """A pytest fixture to set up and tear down the test environment."""
    if CX_HOME.exists():
        shutil.rmtree(CX_HOME)

    # Run `cx init` once to set up the environment for all tests in this module.
    init_result = subprocess.run("cx init", shell=True, capture_output=True, text=True)
    assert init_result.returncode == 0, (
        f"Setup failed: cx init failed with stderr: {init_result.stderr}"
    )

    yield

    if CX_HOME.exists():
        shutil.rmtree(CX_HOME)


def test_init_command_succeeds():
    """
    Tests that the environment setup by the fixture is valid for the NEW GitHub example.
    """
    # --- THIS IS THE FIX ---
    # Assert that the correct GitHub connection and blueprint were created.
    assert (CX_HOME / "connections" / "github.conn.yaml").exists()
    assert (
        CX_HOME / "blueprints" / "community" / "github" / "v0.1.0" / "blueprint.cx.yaml"
    ).exists()
    # --- END FIX ---


@pytest.mark.asyncio
async def test_blueprint_action_with_mocked_api(tmp_path: Path, mocker: MockerFixture):
    """
    Tests the end-to-end blueprint execution flow for the GitHub blueprint
    by mocking the external HTTP request.
    """
    # 1. Define the predictable API response for the GitHub user endpoint.
    mock_user_data = {
        "login": "torvalds",
        "id": 1024025,
        "name": "Linus Torvalds",
        "company": "Linux Foundation",
    }

    # 2. Mock the `httpx.AsyncClient.request` method.
    mock_response = mocker.Mock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = mock_user_data
    mock_response.content = json.dumps(mock_user_data).encode("utf-8")
    mocker.patch("httpx.AsyncClient.request", return_value=mock_response)

    # 3. Create a temporary workflow script that uses the correct connection and action.
    script_content = {
        "name": "E2E Test Script for GitHub",
        "steps": [
            {
                "id": "get_github_user",
                "name": "Get GitHub User",
                "connection_source": "user:github",  # <-- Use the correct connection
                "run": {
                    "action": "run_declarative_action",
                    "template_key": "getUser",  # <-- Use the correct action
                    "context": {"username": "torvalds"},
                },
            }
        ],
    }

    script_file = tmp_path / "test_github.connector.yaml"
    script_file.write_text(yaml.dump(script_content))

    # 4. Execute the script IN-PROCESS.
    service = ConnectorService()
    results = await service.run_script(script_file)

    # 5. Assert the results based on the new script and mock data.
    assert "Get GitHub User" in results
    user_data = results["Get GitHub User"]
    assert user_data["id"] == 1024025
    assert user_data["login"] == "torvalds"
