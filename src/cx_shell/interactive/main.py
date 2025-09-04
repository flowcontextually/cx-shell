import asyncio
from pathlib import Path

from prompt_toolkit import PromptSession
from prompt_toolkit.filters import Condition
from prompt_toolkit.history import FileHistory
from prompt_toolkit.key_binding import KeyBindings

from .completer import CxCompleter
from .executor import CommandExecutor, console
from .session import SessionState


def start_repl():
    """
    Starts the main Read-Eval-Print-Loop (REPL) for the interactive shell.

    This function sets up the entire interactive environment, including history,
    state management, command completion, and key bindings. It contains the
    main asynchronous loop that waits for user input and dispatches it to the
    CommandExecutor.

    Crucially, it is designed to handle session state replacement, allowing the
    'load' command to completely swap out the active session.
    """
    history_file = Path.home() / ".cx_history"

    # Initialize the core components with the initial session state.
    # These will be updated if a new session is loaded.
    state = SessionState()
    completer = CxCompleter(state)
    executor = CommandExecutor(state)

    bindings = KeyBindings()

    # Create the PromptSession, which is the heart of the REPL interface.
    prompt_session = PromptSession(
        history=FileHistory(str(history_file)),
        completer=completer,
        complete_while_typing=True,
    )

    # Define a custom key binding for the "Enter" key.
    @bindings.add(
        "enter",
        # This filter ensures the binding only applies when the completion menu is visible.
        filter=Condition(
            lambda: prompt_session.default_buffer.complete_state is not None
        ),
    )
    def _(event):
        """
        When Enter is pressed while the completion menu is open, this function
        applies the currently selected completion instead of submitting the command.
        This provides a more intuitive IDE-like experience.
        """
        completion = event.current_buffer.complete_state.current_completion
        if completion:
            event.current_buffer.apply_completion(completion)

    # Assign the completed key bindings to the session.
    prompt_session.key_bindings = bindings

    async def repl_main():
        nonlocal state, completer, executor
        while state.is_running:
            try:
                command_text = await prompt_session.prompt_async("cx> ")
                if not command_text or not command_text.strip():
                    continue
                if command_text.strip().lower() in ["exit", "quit"]:
                    state.is_running = False
                    continue

                # The execute method will now return either None or a new SessionState
                new_state = await executor.execute(command_text)

                if isinstance(new_state, SessionState):
                    state = new_state
                    # Re-point the executor and completer to the new state object
                    executor.state = state
                    completer.state = state
                    console.print("[bold yellow]Session restored.[/bold yellow]")

            except KeyboardInterrupt:
                print()
                continue
            except EOFError:
                print()
                state.is_running = False

    # Run the main asynchronous event loop.
    asyncio.run(repl_main())

    print("Exiting Contextual Shell. Goodbye!")
