# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
