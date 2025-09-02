from prompt_toolkit.completion import Completer, Completion
from prompt_toolkit.document import Document

from .session import SessionState


class CxCompleter(Completer):
    """
    A custom completer for the Contextual Shell.

    Provides context-aware suggestions for built-in commands, active connection
    aliases, and dot-notation actions on those aliases.
    """

    def __init__(self, state: SessionState):
        self.state = state
        # In a future version, this list will be dynamically populated by inspecting
        # the connection's blueprint. For now, it's hardcoded for the MVP.
        self.supported_actions = ["browse", "read"]
        self.builtin_commands = ["connect", "connections", "help", "exit", "quit"]

    def get_completions(self, document: Document, complete_event):
        """
        The main generator function called by prompt_toolkit to get suggestions.
        """
        text_before_cursor = document.text_before_cursor
        word_before_cursor = document.get_word_before_cursor()

        # --- CONTEXT 1: Dot-Notation Action Completion ---
        # This context is active if the user has typed an alias and a dot,
        # for example: `fs.` or `fs.br`
        if "." in text_before_cursor and " " not in text_before_cursor:
            try:
                alias, action_prefix = text_before_cursor.split(".", 1)

                # Check if the alias is a valid, active connection
                if alias in self.state.connections:
                    # Find all supported actions that start with the prefix
                    for action in self.supported_actions:
                        if action.startswith(action_prefix):
                            # This is the crucial part: the start_position is negative
                            # the length of the text *after* the dot. This tells the
                            # completer to only replace the partial action name.
                            yield Completion(
                                text=action, start_position=-len(action_prefix)
                            )
                # We return here because we are confident this is the only
                # relevant completion context.
                return
            except ValueError:
                # Ignore cases with multiple dots, etc.
                pass

        # --- CONTEXT 2: First-Word Command/Alias Completion ---
        # This context is active if the user is typing the first word on the line.
        if " " not in text_before_cursor:
            # Suggest built-in commands
            for command in self.builtin_commands:
                if command.startswith(word_before_cursor):
                    yield Completion(
                        text=command, start_position=-len(word_before_cursor)
                    )

            # Suggest active connection aliases
            for alias in self.state.connections:
                if alias.startswith(word_before_cursor):
                    # We can add a meta-description to tell the user what it is
                    yield Completion(
                        text=alias,
                        start_position=-len(word_before_cursor),
                        display_meta="connection alias",
                    )

        # --- Future Contexts ---
        # In the future, we could add more logic here to handle things like
        # path completion inside a command, e.g., fs.browse('/ho<TAB>')
