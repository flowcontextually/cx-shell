import shutil
from pathlib import Path
import json
import subprocess

import pytest
import yaml
from pytest_mock import MockerFixture

# We now import our application's core service to test it directly.
from cx_shell.engine.connector.service import ConnectorService

CX_HOME = Path.home() / ".cx"


@pytest.fixture(scope="module", autouse=True)
def setup_environment():
    """A pytest fixture to set up and tear down the test environment."""
    # Run `cx init` using a subprocess just once to set up the environment.
    # This is a safe and robust way to prepare the ground truth for our tests.
    if CX_HOME.exists():
        shutil.rmtree(CX_HOME)

    init_result = subprocess.run("cx init", shell=True, capture_output=True, text=True)
    assert init_result.returncode == 0, (
        f"Setup failed: cx init failed with stderr: {init_result.stderr}"
    )

    yield

    if CX_HOME.exists():
        shutil.rmtree(CX_HOME)


def test_init_command_succeeds():
    """
    Tests that the environment setup by the fixture is valid.
    """
    assert (CX_HOME / "connections" / "petstore.conn.yaml").exists()
    assert (
        CX_HOME / "blueprints" / "community" / "petstore" / "v2.0" / "blueprint.cx.yaml"
    ).exists()


# --- THIS IS THE NEW, DEFINITIVE TEST ---
@pytest.mark.asyncio
async def test_blueprint_action_with_mocked_api(tmp_path: Path, mocker: MockerFixture):
    """
    Tests the end-to-end blueprint execution flow in-process, allowing our
    API mock to work correctly.
    """
    # 1. Define the predictable API response.
    mock_pet_data = {
        "id": 2,
        "name": "doggie",  # This is the name we expect
        "status": "available",
    }

    # 2. Mock the `httpx.AsyncClient.request` method. This is the same as before.
    mock_response = mocker.Mock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = mock_pet_data
    mock_response.content = json.dumps(mock_pet_data).encode("utf-8")
    mocker.patch("httpx.AsyncClient.request", return_value=mock_response)

    # 3. Create a temporary workflow script file.
    script_content = {
        "name": "E2E Test Script for Petstore",
        "steps": [
            {
                "id": "get_pet_2",
                "name": "Get Pet By ID 2",
                "connection_source": "user:petstore",
                "run": {
                    "action": "run_declarative_action",
                    "template_key": "getPetById",
                    "context": {"petId": 2},
                },
            }
        ],
    }
    script_file = tmp_path / "test_petstore.connector.yaml"
    script_file.write_text(yaml.dump(script_content))

    # 4. Execute the script IN-PROCESS.
    # We instantiate our own service and call its method directly.
    service = ConnectorService()
    results = await service.run_script(script_file)

    # 5. Assert the results. The mock is now guaranteed to have been called.
    assert "Get Pet By ID 2" in results
    pet_data = results["Get Pet By ID 2"]
    assert pet_data["id"] == 2
    assert pet_data["name"] == "doggie"  # This will now pass.
