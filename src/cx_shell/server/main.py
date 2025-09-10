# /home/dpwanjala/repositories/cx-shell/src/cx_shell/server/main.py
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from typing import Any, Optional
import structlog
import uuid

from ..interactive.session import SessionState
from ..interactive.executor import CommandExecutor
from ..interactive.output_handler import IOutputHandler
from ..engine.connector.utils import safe_serialize
from ..interactive.commands import Command

logger = structlog.get_logger(__name__)
app = FastAPI()


class WebSocketHandler(IOutputHandler):
    """
    An IOutputHandler implementation that sends structured JSON messages
    over a WebSocket connection. This is the bridge between the cx-engine
    and the web UI.
    """

    def __init__(
        self, websocket: WebSocket, command_id: str, executor: CommandExecutor
    ):
        self.websocket = websocket
        self.command_id = command_id
        self.executor = executor  # Store executor to access session state

    async def handle_result(
        self, result: Any, executable: Optional[Command], options: Optional[dict] = None
    ):
        """Processes the final result and sends it as a success or error message."""
        try:
            if isinstance(result, dict) and "error" in result:
                await self.send_error(result["error"])
            else:
                # Pass the executable object and the current session state to send_success
                await self.send_success(result, executable, self.executor.state)
        except Exception as e:
            logger.error("Error in WebSocketHandler", exc_info=True)
            await self.send_error(f"Error in WebSocketHandler: {e}")

    async def send_message(self, msg_type: str, payload: Any):
        """Utility to send a structured message over the WebSocket."""
        await self.websocket.send_json(
            {
                "type": msg_type,
                "command_id": self.command_id,
                "payload": safe_serialize(payload),
            }
        )

    async def send_error(self, error_message: str):
        await self.send_message("RESULT_ERROR", {"error": error_message})

    async def send_success(
        self, data: Any, executable: Optional[Command], session_state: SessionState
    ):
        """
        Sends a success message, packaging the command's result and the
        latest session state for UI synchronization.
        """
        is_list_command = (
            hasattr(executable, "subcommand") and executable.subcommand == "list"
        ) or (hasattr(executable, "command") and executable.command == "connections")

        # For list commands, the data IS the payload. For others, it's nested.
        result_payload = data if is_list_command else {"result": data}

        # Always include the latest session state for the UI to sync.
        connections = [
            {"alias": a, "source": s} for a, s in session_state.connections.items()
        ]
        variables = [
            {"name": n, "type": type(v).__name__, "preview": repr(v)[:100]}
            for n, v in session_state.variables.items()
        ]

        full_payload = {
            "result": result_payload,
            "new_session_state": {
                "connections": connections,
                "variables": variables,
            },
        }
        await self.send_message("RESULT_SUCCESS", full_payload)


@app.get("/health")
async def health_check():
    """A simple endpoint to confirm the server is running."""
    return {"status": "ok"}


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """Manages a persistent WebSocket connection for a single client session."""
    await websocket.accept()
    client_id = f"{websocket.client.host}:{websocket.client.port}"
    session_id = str(uuid.uuid4())
    log = logger.bind(client_id=client_id, session_id=session_id)
    log.info("WebSocket client connected.")

    session_state = SessionState(is_interactive=False)
    # The handler is now initialized per-command, so we pass None here.
    executor = CommandExecutor(session_state, output_handler=None)

    try:
        while True:
            data = await websocket.receive_json()
            command_text = data.get("payload", {}).get("command_text")
            command_id = data.get("command_id", str(uuid.uuid4()))

            if not command_text:
                continue

            log.info(
                "Received command from client.",
                command_text=command_text,
                command_id=command_id,
            )

            # Create a new handler for THIS specific command run.
            output_handler = WebSocketHandler(websocket, command_id, executor)
            executor.output_handler = output_handler

            await output_handler.send_message("COMMAND_STARTED", {})

            new_state_or_result = await executor.execute(command_text)

            # If execute returns a new SessionState (from 'session load'), update our state
            if isinstance(new_state_or_result, SessionState):
                executor.state = new_state_or_result
                # Send a special message to the client to confirm and sync UI
                await output_handler.send_success(
                    {"message": "Session restored successfully."}, None, executor.state
                )

    except WebSocketDisconnect:
        log.info("WebSocket client disconnected.")
    except Exception:
        log.error(
            "An unexpected error occurred in the WebSocket endpoint.", exc_info=True
        )
    finally:
        log.info("Closing WebSocket session.")
