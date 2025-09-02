import asyncio
from pathlib import Path

from prompt_toolkit import PromptSession
from prompt_toolkit.filters import Condition
from prompt_toolkit.history import FileHistory
from prompt_toolkit.key_binding import KeyBindings

from .completer import CxCompleter
from .executor import CommandExecutor
from .session import SessionState


def start_repl():
    """
    Starts the main Read-Eval-Print-Loop for the interactive shell.
    """
    history_file = Path.home() / ".cx_history"
    state = SessionState()
    completer = CxCompleter(state)
    executor = CommandExecutor(state)
    bindings = KeyBindings()

    # Create the PromptSession first so we can reference it in the filter.
    prompt_session = PromptSession(
        history=FileHistory(str(history_file)),
        completer=completer,
        complete_while_typing=True,
    )

    # --- THIS IS THE DEFINITIVE FIX, CONFIRMED BY DOCUMENTATION ---
    # The condition is true only when the completion menu is visible.
    # The correct check is for the existence of the `complete_state` object.
    @bindings.add(
        "enter",
        filter=Condition(
            lambda: prompt_session.default_buffer.complete_state is not None
        ),
    )
    def _(event):
        """
        When the Enter key is pressed AND the completion menu is visible,
        this function applies the currently selected completion instead of
        submitting the command.
        """
        # Get the currently selected completion.
        completion = event.current_buffer.complete_state.current_completion
        if completion:
            # Use the buffer's own method to apply the completion.
            event.current_buffer.apply_completion(completion)

    # --- END FIX ---

    # Now that the bindings are complete, assign them to the session.
    prompt_session.key_bindings = bindings

    async def repl_main():
        while state.is_running:
            try:
                command_text = await prompt_session.prompt_async("cx> ")

                if not command_text or not command_text.strip():
                    continue

                if command_text.strip().lower() in ["exit", "quit"]:
                    state.is_running = False
                    continue

                await executor.execute(command_text)

            except KeyboardInterrupt:
                print()
                continue
            except EOFError:
                print()
                state.is_running = False

    asyncio.run(repl_main())

    print("Exiting Contextual Shell. Goodbye!")
