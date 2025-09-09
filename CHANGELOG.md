# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.5.6] - 2025-09-09

### Added

- **Multi-Format Blueprint Compiler:** The `cx compile` command is now powered by a new, extensible dispatcher/adapter architecture.
- **Google API Discovery Support:** The compiler can now ingest and parse Google API Discovery Documents, enabling the creation of blueprints for services like Google Drive and Calendar.

### Changed

- **Architectural Refactoring:** The compiler logic was refactored from a single monolithic script (`openapi_compiler.py`) into a clean, modular system (`spec_compiler.py`, `openapi_adapter.py`, `google_discovery_adapter.py`).
- **Improved Portability:** Introduced the `app-asset:` URI scheme and updated the `ScriptEngine` to correctly resolve asset paths (e.g., `.sql` files) within installed applications, making flows truly portable.

### Fixed

- **SQL `IN` Clause:** Resolved a critical `sqlalchemy.exc.ArgumentError` by implementing manual parameter expansion for list values in SQL `IN` clauses, ensuring compatibility with `pyodbc` and other restrictive drivers.
- **Pydantic Schema Generation:** Fixed multiple bugs in the OpenAPI adapter that caused it to fail on complex specifications (e.g., Microsoft Graph), including silent skipping of schemas and incorrect generation of forward references.

## [0.5.5] - 2025-09-09

### Fixed

- **Linux Binary Portability:** The Linux executable is now built inside a Docker container based on an older OS (Debian Bullseye) to ensure broad compatibility with a wide range of `glibc` versions. This resolves critical startup failures on common Long-Term Support (LTS) server distributions like Ubuntu 20.04.
- **CI/CD Build Pipeline:** Hardened the entire release workflow to resolve multiple build failures.
  - Corrected the Docker image tag to a stable, available version.
  - Added missing `git` and `binutils` dependencies to the Docker build environment to support dependency installation and PyInstaller.
  - Replaced the unreliable `zip` command on Windows runners with the robust, built-in `Compress-Archive` PowerShell command.
  - Switched the final release step from using a static `CHANGELOG.md` path to using GitHub's superior automatic release note generation.

## [0.5.4] - 2025-09-08

### Fixed

- **`cx upgrade` Command:** Resolved a deadlock that caused the `upgrade` command to hang when prompting for user confirmation. The command is now fully functional and robust.

## [0.5.3] - 2025-09-08

### Added

- **Application Lifecycle Management:** Implemented the full suite of `cx app` commands (`install`, `uninstall`, `list`, `package`, `search`) for managing self-contained, distributable application packages.
- **Multi-Source Installation:** The `app install` command now robustly handles installation from the public registry (via `--id`), local archive files (via `--path`), and direct download URLs (via `--url`).
- **Interactive App Onboarding:** The `cx app install` command now features an interactive wizard that guides users through setting up all required connections declared in an application's manifest.
- **Installation Manifest:** The shell now maintains a local manifest (`~/.cx/apps.json`) to track installed applications and their associated assets, enabling clean uninstallation.

### Fixed

- **Parser Robustness:** Refactored the shell's Lark grammar and transformer to consistently use named arguments (`--flag value`) for all complex commands, eliminating parsing ambiguity with special characters in arguments like URLs.
- **CLI Output Polish:** The presentation layer now intelligently suppresses redundant JSON status messages for commands that produce their own rich output (like tables), creating a cleaner user experience.

## [0.5.2] - 2025-09-08

### Added

- **Self-Upgrade Capability:** Introduced the `cx upgrade` command to allow the application to automatically check for, download, and install the latest version, providing a seamless update experience.

### Fixed

- **PyInstaller Bundling:** Resolved a critical `FileNotFoundError` in the distributed binaries by correctly bundling non-code assets (like the `.lark` grammar file and other `assets`).
- **Application Startup Performance:** Drastically improved startup time for non-agentic commands by implementing lazy loading for all expensive components (`fastembed`, `tiktoken`, `AgentOrchestrator`).
- **Version Display:** Fixed an issue where `cx --version` would report "unknown" in the distributed binary.

## [0.5.1] - 2025-09-08

### Fixed

- **Application Startup Performance:** Resolved a multi-second startup delay for non-agentic commands (like `cx --version`) by implementing lazy loading for all expensive components (`fastembed`, `tiktoken`, `AgentOrchestrator`). Startup is now nearly instantaneous.
- **Agent Onboarding Deadlock:** Fixed a critical bug where the `//` translate feature would hang indefinitely on first use when trying to set up an LLM connection. The fix ensures that interactive prompts do not conflict with the terminal's UI spinner.
- **Agent Reliability:** Greatly improved the reliability of the `AnalystAgent` by providing few-shot examples in its system prompt, preventing `ValidationError` crashes when processing command outputs.

## [0.5.0] - 2025-09-08

### Added

- **Collaborative Agent (CARE Engine):** Introduced the full, multi-step agentic reasoning loop with the `agent <goal>` command.
- **Hierarchical Agent Architecture:** Implemented the full Composite Agent Reasoning Engine (CARE) with three specialist agents: Planner, ToolSpecialist, and Analyst.
- **Human-in-the-Loop Workflow:** The agent now presents its plan, generates commands, and requires user confirmation before execution for safety and transparency.
- **Multi-Gate Validation:** Agent-generated commands are now validated through a three-gate system (plan sanity, static syntax check, and dry-run simulation) to ensure reliability.
- **Self-Correcting LLM Calls:** The `instructor` library now automatically retries LLM calls with validation feedback, improving the reliability of structured data generation.

### Fixed

- **Agent Command Generation:** Completely resolved all issues where agents would generate syntactically invalid commands (e.g., with a `cx` prefix) by providing better-constrained prompts and examples.
- **Analyst Reliability:** Fixed `ValidationError` crashes in the Analyst agent by providing few-shot examples in the prompt, ensuring it always produces a valid `belief_update`.
- **Async Deadlock:** Resolved a critical hang/deadlock in the interactive shell that occurred when agent features tried to perform synchronous file I/O.
- **Command Execution:** Fixed `TypeError` and `AttributeError` bugs in the `CommandExecutor` that prevented agent-generated commands from being executed.

## [0.4.0] - 2025-09-04

### Added

- **Agentic Shell Foundation:** Introduced the core architecture for AI assistance.
- **Translate Feature (`// <prompt>`):** Added a "fast path" co-pilot for instantaneous, inline command suggestions.
- **On-Demand Onboarding:** The Translate feature now automatically triggers an interactive wizard to configure required LLM provider connections if one is not active.
- **Provider-Agnostic Agent Backend:** The agent's core is now powered by the `cx` platform's own Blueprint system, making it extensible to any LLM provider.
- **Bundled LLM Blueprints:** `cx init` now includes pre-packaged `community` blueprints for OpenAI, Anthropic, and Google Gemini.
- **Asynchronous Process Management:** Added `cx process` command group to the CLI for managing long-running background tasks.

## [0.3.0] - 2025-09-04

This release introduces the **Application Ecosystem**, a major new feature for packaging, distributing, and managing complete, end-to-end solutions.

### Added

- **Application Management (`cx app`):** A full suite of commands in both the CLI and interactive REPL (`install`, `list`, `uninstall`, `search`, `sync`, `package`) to manage applications.
- **Public Application Registry:** Users can now discover and install official and community-vetted applications using commands like `cx app search`.
- **Private Application Support:** `cx app install` supports installing applications directly from a URL to a private Git repository archive.
- **Automatic Dependency Resolution:** The installer automatically resolves and downloads any required blueprint dependencies declared in an application's manifest.
- **Interactive Setup Wizard:** Installing an application now launches a guided wizard to help the user configure all necessary connections.
- **CLI-REPL Parity:** Added `flow list`, `query list`, and `script list` to the main CLI for a consistent experience with the interactive shell.

## [0.2.1] - 2025-09-04

This is a major bugfix and feature-enhancement release that creates a seamless, robust, and elegant experience for building monolithic flows.

### Added

- **Native `run_transform` Action:** Flows (`.flow.yaml`) now have a first-class `run_transform` action, eliminating the need for helper Python scripts to run transformer workflows.
- **Declarative Payload Engine:** Blueprints now support a powerful `_mapping` block that allows the engine to automatically and declaratively transform simple user context into complex, nested, and validated API payloads using the Pydantic schema as the instruction set.

### Fixed

- **Dynamic Schema Loading:** Completely resolved all `PydanticUndefinedAnnotation` errors (e.g., `name 'Optional' is not defined`) that occurred when dynamically loading blueprint schemas with forward references.
- **Jinja Native Type Evaluation:** The flow engine now correctly evaluates single-variable Jinja templates (e.g., `{{ my_list }}`) to their native Python type instead of incorrectly casting them to strings.
- **Path Expansion:** The engine now automatically expands the home directory character (`~`) in all file paths used within flows and blueprints.
- **Connection-less Steps:** The flow engine now correctly handles steps that do not require a `connection_source` (like `run_transform`), fixing an `AttributeError`.

## [0.2.0] - 2025-09-03

This is a landmark release, transforming the `cx` shell from a simple command runner into a powerful, stateful, and persistent "Workspace IDE" for interactive data workflows.

### Added

- **Full Asset-Oriented Management Suite:** Introduced a comprehensive set of commands to manage the entire user workspace directly from the REPL:
  - `session [list|save|load|rm|status]`: For saving, restoring, and managing persistent workspace sessions.
  - `var [list|rm]`: For listing and deleting in-memory session variables.
  - `flow [list|run]`: For discovering and executing reusable `.flow.yaml` workflows.
  - `query [list|run]`: For discovering and running reusable `.sql` queries against active connections.
  - `script [list|run]`: For discovering and running reusable `.py` scripts.
  - `connection [list|create]`: For managing on-disk connection files from within the REPL.
- **Universal `open` Command:** A powerful utility to open workspace assets (`flow`, `query`, `config`, etc.) in the user's default application or a specified one (`--in vscode`). It is WSL-aware for a seamless experience and can open dynamic URLs from variables.
- **Universal Output Formatter:** A flexible flag system to control the presentation of any command's output.
  - `--output table`: Renders list-based results as a formatted table.
  - `--columns <cols>`: Selects specific columns for table view.
  - `--query <jmespath>`: Filters and reshapes JSON output using a JMESPath expression before rendering.
- **Pipelining (`|`):** The output of any data-producing command can now be piped as the input to the next command (e.g., `gh.read(...) | script run ...`).
- **Session Variables & Context Injection:**
  - Users can now assign the result of any command or pipeline to a variable (e.g., `my_var = ...`).
  - Stored variables can be used in subsequent commands via Jinja2 templating (e.g., `gh.read("{{ my_var.url }}")`).
- **Rich Object Inspection (`inspect`):** Replaced the ambiguous `?` syntax with an explicit `inspect <var>` command that displays a formatted summary of any session variable.
- **Parentheses for Precedence:** The shell grammar now supports using `(...)` to group commands and control the order of operations in complex pipelines.

### Changed

- **Upgraded Parsing Engine:** The entire interactive shell was refactored from an ad-hoc parser to a robust, extensible foundation using the **Lark** parser and the **Command Pattern**. This improves reliability and maintainability.
- **Polished `help` Command:** The in-shell `help` command has been completely redesigned to be more comprehensive and document all new features.
- **Improved Table Rendering:** The `--output table` formatter now correctly handles both very wide and very narrow tables for optimal display.

### Fixed

- Resolved numerous `Reduce/Reduce` grammar collisions in Lark related to commands with optional arguments.
- Fixed `LexError` and `GrammarError` issues by standardizing the use of Terminals vs. Rules.
- Fixed `AttributeError` in `rich.Table` rendering by using the correct `box` style object instead of a string.
- Fixed `TypeError` and `VisitError` bugs in the command transformer related to rules with no non-literal children.
- Fixed `RuntimeError` caused by using synchronous `prompt_toolkit` functions inside the main asyncio event loop for `session rm` and `connection create`.
- Corrected JMESPath querying logic to correctly unpack `VfsFileContentResponse` objects before applying a query.
- Made the `QueryManager` context-aware by resolving the `--on <alias>` flag against the current `SessionState`.
- Made the `ScriptManager` robust against non-serializable data types (like `datetime`) in piped input by using `safe_serialize`.
