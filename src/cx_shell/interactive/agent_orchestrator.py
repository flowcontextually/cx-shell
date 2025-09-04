from typing import Optional

from rich.console import Console
from rich.panel import Panel
from prompt_toolkit.shortcuts import PromptSession
from prompt_toolkit.formatted_text import HTML

from ..interactive.session import SessionState
from ..interactive.context_engine import DynamicContextEngine
from ..management.belief_manager import BeliefManager
from ..feedback_logger import FeedbackLogger
from ..agent.planner_agent import PlannerAgent
from ..agent.tool_specialist_agent import ToolSpecialistAgent
from ..agent.analyst_agent import AnalystAgent

# --- FIX: Import the missing PlanStep schema ---
from ..data.agent_schemas import LLMResponse, PlanStep


# Forward declaration to avoid circular import with executor
class CommandExecutor:
    pass


CONSOLE = Console()


class AgentOrchestrator:
    """
    The core orchestrator for the CARE agent. Manages the reasoning loop,
    specialist agents, and user interaction.
    """

    def __init__(self, state: SessionState, executor: "CommandExecutor"):
        self.state = state
        self.executor = executor
        self.context_engine = DynamicContextEngine(state)
        self.belief_manager = BeliefManager()
        self.feedback_logger = FeedbackLogger()
        self.prompt_session = PromptSession()

        # Initialize specialist agents. In a real app, these would be configured
        # from agents.config.yaml. For now, we instantiate them directly.
        self.planner = PlannerAgent(state, executor.service)
        self.tool_specialist = ToolSpecialistAgent(state, executor.service)
        self.analyst = AnalystAgent(state, executor.service)

    async def start_session(self, goal: str):
        """Initiates a new, stateful, multi-step reasoning session."""
        CONSOLE.print(
            Panel(
                f"[bold]Goal:[/bold] {goal}",
                title="Agent Session Started",
                border_style="blue",
            )
        )

        try:
            beliefs = self.belief_manager.initialize_beliefs(self.state, goal)

            # --- Main Reasoning Loop ---
            while True:
                # 1. Plan (if necessary)
                if not beliefs.plan or any(s.status == "failed" for s in beliefs.plan):
                    strategic_context = self.context_engine.get_strategic_context(
                        goal, beliefs
                    )
                    new_plan = await self.planner.generate_plan(goal, strategic_context)
                    self.belief_manager.update_beliefs(
                        self.state,
                        [
                            {
                                "op": "replace",
                                "path": "/plan",
                                "value": [p.model_dump() for p in new_plan],
                            }
                        ],
                    )
                    beliefs = self.belief_manager.get_beliefs(
                        self.state
                    )  # Refresh beliefs

                # 2. Find next step
                next_step_index, next_step = -1, None
                for i, step in enumerate(beliefs.plan):
                    if step.status == "pending":
                        next_step_index, next_step = i, step
                        break

                if not next_step:
                    CONSOLE.print(
                        Panel(
                            "[bold green]Task Complete.[/bold green]",
                            border_style="green",
                        )
                    )
                    break  # Exit loop if no pending steps

                # 3. Act
                llm_response = await self.act_on_step(next_step)

                # 4. Present & Confirm (The Safety Gate)
                confirmed, user_command = await self.present_and_confirm(llm_response)

                if not confirmed:
                    CONSOLE.print("[yellow]Action cancelled by user.[/yellow]")
                    # Log the rejection for future learning
                    if llm_response.cx_command:
                        self.feedback_logger.log_user_correction(
                            next_step.step, llm_response.cx_command, user_command or ""
                        )
                    if not user_command:
                        break  # End session if user just cancels
                    command_to_run = user_command
                else:
                    command_to_run = llm_response.cx_command

                # 5. Execute
                observation = await self.executor._execute_executable(command_to_run)

                # 6. Analyze & Update Beliefs
                analyst_response = await self.analyst.analyze_observation(
                    next_step.step, observation
                )
                self.belief_manager.update_beliefs(
                    self.state, analyst_response.belief_update
                )
                # TODO: Log summary to history DB

                # Refresh beliefs for the next iteration of the loop
                beliefs = self.belief_manager.get_beliefs(self.state)

        except Exception as e:
            CONSOLE.print(
                f"[bold red]Agentic session failed unexpectedly:[/bold red] {e}"
            )
        finally:
            self.belief_manager.end_session(self.state)

    # --- FIX: Removed quotes from 'PlanStep' as it is now directly imported ---
    async def act_on_step(self, step: PlanStep) -> LLMResponse:
        """Invokes the ToolSpecialist to generate a command for a plan step."""
        # For simplicity, we assume the connection alias can be inferred or is global.
        # A real implementation would need to determine which tool/connection to use.
        # Let's assume the user has a 'gh' connection for now.
        tactical_context = self.context_engine.get_tactical_context("gh")  # Placeholder
        return await self.tool_specialist.generate_command(step.step, tactical_context)

    async def present_and_confirm(
        self, llm_response: LLMResponse
    ) -> (bool, Optional[str]):
        """Presents the agent's plan and command, and awaits user confirmation."""

        command_to_display = llm_response.cx_command or "[No command generated]"

        CONSOLE.print(
            Panel(
                f"[dim]Reasoning:[/dim] {llm_response.reasoning}\n\n"
                f"[bold]Next Command:[/bold]\n> {command_to_display}",
                title="Agent Plan",
                border_style="yellow",
            )
        )

        response = await self.prompt_session.prompt_async(
            HTML("<b>Execute?</b> [<b>Y</b>es/<b>n</b>o/<b>e</b>dit]: "),
        )
        response = response.lower().strip()

        if response == "n" or response == "no":
            return False, None
        if response == "e" or response == "edit":
            edited_command = await self.prompt_session.prompt_async(
                "> ", default=command_to_display
            )
            return False, edited_command.strip()

        return True, None  # Default to Yes

    async def run_co_pilot(self, prompt: str):
        """Runs the fast-path, stateless co-pilot for command suggestion."""
        # This would call the co_pilot role of the ToolSpecialist
        # and replace the content of the prompt_toolkit buffer.
        # This requires deeper integration with the REPL loop in main.py.
        CONSOLE.print("[dim]Co-pilot feature not yet fully implemented.[/dim]")
