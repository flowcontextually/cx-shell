from typing import Optional, cast

from rich.console import Console
from rich.panel import Panel
from prompt_toolkit.shortcuts import PromptSession
from prompt_toolkit.formatted_text import HTML
import yaml

from ..interactive.session import SessionState
from ..interactive.context_engine import DynamicContextEngine
from ..management.belief_manager import BeliefManager
from ..feedback_logger import FeedbackLogger
from ..agent.planner_agent import PlannerAgent
from ..agent.tool_specialist_agent import ToolSpecialistAgent
from ..agent.analyst_agent import AnalystAgent
from ..data.agent_schemas import LLMResponse, PlanStep, AgentBeliefs


class CommandExecutor:
    pass


CONSOLE = Console()


class AgentOrchestrator:
    """The core orchestrator for the CARE agent. Manages the reasoning loop, and the translate feature."""

    def __init__(self, state: SessionState, executor: "CommandExecutor"):
        self.state = state
        self.executor = executor
        self.context_engine = DynamicContextEngine(state)
        self.belief_manager = BeliefManager()
        self.feedback_logger = FeedbackLogger()
        self.prompt_session = PromptSession()

        self.planner = PlannerAgent(state, executor.service)
        self.tool_specialist = ToolSpecialistAgent(state, executor.service)
        self.analyst = AnalystAgent(state, executor.service)

    async def _ensure_agent_connection(self, role_name: str) -> bool:
        """
        Checks for a required agent connection, prompting the user to activate an
        existing one or create a new one if necessary.
        """
        if not self.tool_specialist.agent_config:
            CONSOLE.print(
                "[bold red]Error:[/bold red] Agent configuration not found or invalid."
            )
            return False

        profile = self.tool_specialist.agent_config.profiles[
            self.tool_specialist.agent_config.default_profile
        ]
        role_config = getattr(profile, role_name)
        alias = role_config.connection_alias

        if alias in self.state.connections:
            return True

        # --- NEW: Intelligent Check for Existing Saved Connections ---
        provider_name = alias.replace("cx_", "")
        blueprint_id_pattern = f"community/{provider_name}@"

        # Find any saved connections that use the right blueprint
        compatible_conns = []
        for conn_file in self.executor.connection_manager.connections_dir.glob(
            "*.conn.yaml"
        ):
            try:
                data = yaml.safe_load(conn_file.read_text())
                if data.get("api_catalog_id", "").startswith(blueprint_id_pattern):
                    compatible_conns.append(data.get("id", "").replace("user:", ""))
            except Exception:
                continue

        if compatible_conns:
            # If we found one or more compatible saved connections, ask the user to activate one.
            CONSOLE.print(
                f"[agent] To use this feature, I need an active '{provider_name}' connection."
            )

            # Use a prompt_toolkit completer for a better UX
            from prompt_toolkit.completion import WordCompleter

            completer = WordCompleter(compatible_conns, ignore_case=True)

            chosen_conn_id = await self.prompt_session.prompt_async(
                HTML(
                    f"Press <b>Enter</b> to activate '<b>{compatible_conns[0]}</b>' or choose another: "
                ),
                completer=completer,
                default=compatible_conns[0],
            )

            if chosen_conn_id in compatible_conns:
                await self.executor.execute_connect(
                    [f"user:{chosen_conn_id}", "--as", alias]
                )
                return alias in self.state.connections
            else:
                # User entered something invalid, proceed to create a new one.
                pass

        # --- Fallback to Create New Connection Flow ---
        feature_name = (
            "the 'Translate' feature (`//`)"
            if role_name == "co_pilot"
            else "the Agent (`agent ...`)"
        )
        CONSOLE.print(
            f"\n[agent] No suitable connection is active. Let's set up a new one for the {feature_name}."
        )

        blueprint_id = f"{blueprint_id_pattern}1.0.0"

        created_conn_id = await self.executor.connection_manager.create_interactive(
            preselected_blueprint_id=blueprint_id
        )

        if created_conn_id:
            await self.executor.execute_connect(
                [f"user:{created_conn_id}", "--as", alias]
            )
        else:
            CONSOLE.print("[yellow]Setup cancelled. Agent cannot proceed.[/yellow]")
            return False

        return alias in self.state.connections

    async def prepare_and_run_translate(self, prompt: str) -> Optional[str]:
        """
        Ensures prerequisites are met and then runs the translation. Returns None on setup failure.
        """
        # --- Stage 1: Prerequisite Check ---
        # This part might trigger interactive prompts.
        is_ready = await self._ensure_agent_connection("co_pilot")
        if not is_ready:
            return None  # Signal to the caller that setup failed.

        # --- Stage 2: Execution (wrapped in a status by the caller) ---
        tactical_context = []
        for alias in self.state.connections:
            if not alias.startswith("cx_"):
                tactical_context.extend(self.context_engine.get_tactical_context(alias))

        llm_response = await self.tool_specialist.generate_command(
            prompt, tactical_context, is_translate=True
        )
        return llm_response.cx_command or ""

    async def start_session(self, goal: str):
        """Initiates a new, stateful, multi-step reasoning session."""
        if not await self._ensure_agent_connection("planner"):
            return

        CONSOLE.print(
            Panel(
                f"[bold]Goal:[/bold] {goal}",
                title="Agent Session Started",
                border_style="blue",
            )
        )

        try:
            beliefs = self.belief_manager.initialize_beliefs(self.state, goal)

            for _ in range(10):
                if not beliefs.plan or any(s.status == "failed" for s in beliefs.plan):
                    CONSOLE.print("[yellow]Engaging Planner Agent...[/yellow]")
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
                    beliefs = cast(
                        "AgentBeliefs", self.belief_manager.get_beliefs(self.state)
                    )

                next_step: Optional[PlanStep] = None
                for i, step in enumerate(beliefs.plan):
                    if step.status == "pending":
                        next_step = step
                        self.belief_manager.update_beliefs(
                            self.state,
                            [
                                {
                                    "op": "replace",
                                    "path": f"/plan/{i}/status",
                                    "value": "in_progress",
                                }
                            ],
                        )
                        break

                if not next_step:
                    CONSOLE.print(
                        Panel(
                            "[bold green]Task Complete.[/bold green]",
                            border_style="green",
                        )
                    )
                    break

                CONSOLE.print(
                    f"[yellow]Engaging Tool Specialist for step: '{next_step.step}'...[/yellow]"
                )
                llm_response = await self.act_on_step(next_step)

                if not llm_response.cx_command:
                    CONSOLE.print(
                        "[bold red]Agent could not determine a command to run. Ending session.[/bold red]"
                    )
                    break

                confirmed, user_command = await self.present_and_confirm(llm_response)

                command_to_run = (
                    user_command if user_command else llm_response.cx_command
                )
                if not confirmed:
                    CONSOLE.print("[yellow]Action cancelled by user.[/yellow]")
                    self.feedback_logger.log_user_correction(
                        next_step.step, llm_response.cx_command, user_command or ""
                    )
                    if not user_command:
                        break

                observation = await self.executor._execute_executable(command_to_run)

                CONSOLE.print("[yellow]Engaging Analyst Agent...[/yellow]")
                analyst_response = await self.analyst.analyze_observation(
                    next_step.step, observation
                )
                self.belief_manager.update_beliefs(
                    self.state, analyst_response.belief_update
                )

                beliefs = cast(
                    "AgentBeliefs", self.belief_manager.get_beliefs(self.state)
                )

        except Exception as e:
            CONSOLE.print(
                f"[bold red]Agentic session failed unexpectedly:[/bold red] {e}"
            )
        finally:
            self.belief_manager.end_session(self.state)

    async def act_on_step(self, step: PlanStep) -> LLMResponse:
        tactical_context = []
        for alias in self.state.connections:
            if not alias.startswith("cx_"):
                tactical_context.extend(self.context_engine.get_tactical_context(alias))

        return await self.tool_specialist.generate_command(step.step, tactical_context)

    async def present_and_confirm(
        self, llm_response: LLMResponse
    ) -> (bool, Optional[str]):
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

        if response in ("n", "no"):
            return False, None
        if response in ("e", "edit"):
            edited_command = await self.prompt_session.prompt_async(
                "> ", default=command_to_display
            )
            return False, edited_command.strip()

        return True, None
