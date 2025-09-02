# The Contextual Shell (`cx`) by Flow Contextually

<!-- <p align="center"> -->
  <!-- TODO: Record and add a short GIF of the shell in action: -->
  <!-- cx init -> cx -> connect user:petstore --as api -> api.getPetById(petId=1) -->
  <!-- <img src="placeholder.gif" alt="Contextually Shell Demo"> -->
<!-- </p> -->

Welcome to the **Contextual Shell**, a declarative, multi-stage automation platform for modern data and operations teams. `cx` is the core command-line application for the **Contextually** platform, a universal computational fabric where work retains its full context as it flows between different tools, people, and AI agents.

---

## 🚀 Quick Start: The 5-Minute Tutorial

Get your first "win" in under five minutes. This tutorial will guide you through installing `cx`, initializing a sample project, and making your first dynamic API call.

### 1. Installation

Download and install the latest pre-compiled binary for your operating system from our [**GitHub Releases page**](https://github.com/flowcontextually/cx-shell/releases).

**Linux**

```bash
# This script downloads the latest Linux binary, extracts it, and moves it to your path.
curl -sL https://github.com/flowcontextually/cx-shell/releases/download/v0.1.0/cx-v0.1.0-linux-x86_64.tar.gz | tar -xz
sudo mv cx /usr/local/bin/
cx --version
```

**macOS (Intel)**

```bash
# This script downloads the latest macOS (Intel) binary, extracts it, and moves it to your path.
curl -sL https://github.com/flowcontextually/cx-shell/releases/download/v0.1.0/cx-v0.1.0-macos-x86_64.tar.gz | tar -xz
sudo mv cx /usr/local/bin/
cx --version
```

**macOS (Apple Silicon)**

> **Warning:** A native Apple Silicon build is not yet available. You can run the Intel version via Rosetta 2 using the macOS (Intel) instructions.

**Windows (PowerShell)**

```powershell
# This script downloads the latest Windows binary and unzips it.
$url = "https://github.com/flowcontextually/cx-shell/releases/download/v0.1.0/cx-v0.1.0-windows-amd64.zip"
$output = "cx.zip"
Invoke-WebRequest -Uri $url -OutFile $output
Expand-Archive -Path $output -DestinationPath .

# You should now have `cx.exe` in the current directory.
# For system-wide access, move `cx.exe` to a directory in your system's PATH.
./cx.exe --version
```

### 2. Initialize Your Environment

The `cx init` command creates the necessary configuration files and a sample "Petstore API" blueprint inside your home directory at `~/.cx/`.

```bash
cx init
```

### 3. Run Your First Command!

Now, start the interactive shell and connect to the sample API.

```bash
# 1. Start the interactive shell
cx

# 2. Inside the shell, connect to the sample Petstore API with the alias 'api'
cx> connect user:petstore --as api

# 3. Run your first blueprint-driven command!
cx> api.getPetById(petId=1)
```

**Congratulations!** You've just executed a dynamic API call using a pre-compiled blueprint. To see how it works, explore the files created by `cx init` in your `~/.cx/blueprints` directory.

## Core Concepts

- **The Shell:** An interactive REPL that holds the state of your connections, allowing you to chain commands and explore APIs dynamically.
- **Blueprints:** Version-controlled packages that contain the "knowledge" of how to interact with an external service. They are the reusable, shareable core of the ecosystem.
- **The Compiler (`cx compile`):** A powerful tool that takes a machine-readable specification (like OpenAPI) and automatically generates a complete blueprint package.

## 📚 Documentation

For full documentation, tutorials, and architectural deep dives, please visit our [**official documentation site**](https://flowcontextually.github.io/docs/).

## 🤝 Contributing

We welcome contributions of all kinds! The most valuable way to contribute is by adding new integrations to our public [**Blueprint Registry**](https://github.com/flowcontextually/blueprints).

### Setting up a Development Environment

If you'd like to contribute to the core `cx-shell` application, you'll need the following:

- Python 3.12+
- `uv` (installed via `pip install uv`)

The setup is now streamlined. The `cx-core-schemas` dependency is automatically handled.

```bash
# 1. Clone the repository
git clone https://github.com/flowcontextually/cx-shell.git
cd cx-shell

# 2. Create the virtual environment
uv venv

# 3. Install all dependencies
# This automatically fetches cx-core-schemas from GitHub.
uv pip install -e .[all]

# 4. Verify the installation
source .venv/bin/activate
cx --help
```

---

_Licensed under the MIT License._
