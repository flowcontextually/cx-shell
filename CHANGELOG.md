# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
