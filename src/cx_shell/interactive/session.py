from typing import Any, Dict


class SessionState:
    """
    A simple class to hold the state of an interactive cx shell session.

    This object will be created once when the REPL starts and will persist
    until the user exits. It acts as the "memory" for the shell.
    """

    def __init__(self):
        # A dictionary to store active connections, mapping an alias
        # (e.g., "pdb") to the connection's source string (e.g., "user:postgres-prod").
        self.connections: Dict[str, str] = {}

        # A dictionary to store session variables, allowing users to save
        # results and reference them later.
        self.variables: Dict[str, Any] = {}

        # A flag to control the main loop of the REPL.
        self.is_running: bool = True

        print("Welcome to the Contextual Shell (Interactive Mode)!")
        print("Type 'exit' or press Ctrl+D to quit.")
