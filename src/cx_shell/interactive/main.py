# /home/dpwanjala/repositories/cx-shell/src/cx_shell/interactive/main.py

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
    """Starts the main Read-Eval-Print-Loop (REPL) for the interactive shell."""
    history_file = Path.home() / ".cx_history"
    state = SessionState()
    completer = CxCompleter(state)
    executor = CommandExecutor(state)
    bindings = KeyBindings()
    prompt_session = PromptSession(
        history=FileHistory(str(history_file)),
        completer=completer,
        complete_while_typing=True,
    )

    @bindings.add(
        "enter",
        filter=Condition(
            lambda: prompt_session.default_buffer.complete_state is not None
        ),
    )
    def _(event):
        """Applies the current completion instead of submitting."""
        event.current_buffer.complete_state.current_completion.apply_completion(
            event.current_buffer
        )

    prompt_session.key_bindings = bindings

    async def repl_main():
        nonlocal state, completer, executor
        next_prompt_default = ""

        while state.is_running:
            try:
                command_text = await prompt_session.prompt_async(
                    "cx> ", default=next_prompt_default
                )
                next_prompt_default = ""

                if not command_text or not command_text.strip():
                    continue
                if command_text.strip().lower() in ["exit", "quit"]:
                    state.is_running = False
                    continue

                if command_text.strip().startswith("//"):
                    goal = command_text.strip().lstrip("//").strip()

                    # Delegate the entire workflow to the orchestrator,
                    # which now correctly handles spinners and errors.
                    suggestion = await executor.orchestrator.prepare_and_run_translate(
                        goal
                    )

                    if suggestion is not None:
                        next_prompt_default = suggestion

                    continue

                # Normal command execution path
                new_state = await executor.execute(command_text)

                if isinstance(new_state, SessionState):
                    state = new_state
                    executor.state = state
                    completer.state = state
                    console.print("[bold yellow]Session restored.[/bold yellow]")

            except KeyboardInterrupt:
                print()
                continue
            except EOFError:
                print()
                state.is_running = False

    asyncio.run(repl_main())
    print("Exiting Contextual Shell. Goodbye!")
