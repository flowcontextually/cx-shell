from pathlib import Path
import pytest
import yaml
from pytest_mock import MockerFixture
from cx_shell.engine.connector.service import ConnectorService
from cx_core_schemas.api_catalog import ApiCatalog


@pytest.mark.asyncio
async def test_script_engine_executes_blueprint_action_with_mocks(
    clean_cx_home: Path, mocker: MockerFixture
):
    """
    Integration Test: Verifies the ScriptEngine's end-to-end execution of a
    declarative action, with network and blueprint loading fully mocked.
    """
    # Arrange: Create a connection file that points to a blueprint.
    connection_dir = clean_cx_home / "connections"
    connection_dir.mkdir(parents=True)
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

    # Arrange: Mock the blueprint loading to return an in-memory object.
    mock_catalog = ApiCatalog.model_validate(
        {
            "id": "bp:github",
            "name": "GH",
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
    mocker.patch(
        "cx_shell.engine.connector.config.ConnectionResolver.load_blueprint_by_id",
        return_value=mock_catalog,
    )

    # Arrange: Mock the final API network call.
    mock_response = mocker.Mock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = {"login": "torvalds", "id": 1024025}
    mocker.patch("httpx.AsyncClient.request", return_value=mock_response)

    # Arrange: Create the workflow script to be executed.
    script_file = clean_cx_home / "test.flow.yaml"
    script_file.write_text(
        yaml.dump(
            {
                "name": "Test Script",
                "steps": [
                    {
                        "id": "get_user",
                        "name": "Get User",
                        "connection_source": "user:github",
                        "run": {
                            "action": "run_declarative_action",
                            "template_key": "getUser",
                            "context": {"username": "torvalds"},
                        },
                    }
                ],
            }
        )
    )

    # Act: Execute the script via the ConnectorService.
    service = ConnectorService(cx_home_path=clean_cx_home)
    results = await service.run_script(script_file)

    # Assert: Verify the result is correct and contains no errors.
    assert "Get User" in results
    user_data = results["Get User"]
    assert "error" not in user_data
    assert user_data["id"] == 1024025
