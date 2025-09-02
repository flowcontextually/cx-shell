# The Contextual Shell (`cx`) by Contextually

<!-- <p align="center"> -->
  <!-- TODO: Replace this with a short, powerful GIF of the shell in action -->
  <!-- A great example: cx init, cx, connect user:petstore --as api, api.getPetById(petId=1) -->
  <!-- <img src="placeholder.gif" alt="Contextually Shell Demo"> -->
<!-- </p> -->

Welcome to the **Contextual Shell**, a declarative, multi-stage automation platform for modern data and operations teams. `cx` is the core command-line application for the Contextually platform, a universal computational fabric where work retains its full context as it flows between different tools, people, and AI agents.

---

## 🚀 Quick Start: The 5-Minute Tutorial

Get your first "win" in under five minutes. This tutorial will guide you through installing `cx`, initializing a sample project, and making your first dynamic API call.

### 1. Installation

Download the latest pre-compiled binary for your operating system from our [**GitHub Releases page**](https://github.com/flowcontextually/cx-shell/releases).

**Linux & macOS**

```bash
# Replace v0.1.0 with the latest version number
VERSION="v0.1.0"
# For Intel Macs/Linux
ARCH="x86_64"
# For Apple Silicon (M1/M2/M3) Macs, use ARCH="arm64"

curl -L -o cx.tar.gz "https://github.com/flowcontextually/cx-shell/releases/download/${VERSION}/cx-${VERSION}-linux-${ARCH}.tar.gz"
tar -xzf cx.tar.gz
sudo mv cx /usr/local/bin/
cx --version
```

**Windows (using PowerShell)**

```powershell
# Replace v0.1.0 with the latest version number
$version = "v0.1.0"
$output = "cx-setup.exe"
$url = "https://github.com/flowcontextually/cx-shell/releases/download/${version}/cx-setup-${version}.exe"

Invoke-WebRequest -Uri $url -OutFile $output
./$output
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

For full documentation, tutorials, and architectural deep dives, please visit our [**official documentation site**](https://docs.flowcontextually.com).

## 🤝 Contributing

We welcome contributions of all kinds! The most valuable way to contribute is by adding new integrations to our public [**Blueprint Registry**](https://github.com/flowcontextually/blueprints).

### Setting up a Development Environment

If you'd like to contribute to the core `cx-shell` application, you'll need the following:

- Python 3.12+
- `uv` (installed via `pip install uv`)

```bash
# 1. Clone the repository and our schemas library
git clone https://github.com/flowcontextually/cx-shell.git
git clone https://github.com/flowcontextually/shared-schemas.git

# 2. Navigate into the cx-shell directory
cd cx-shell

# 3. Create the virtual environment
uv venv

# 4. Install all dependencies in editable mode
# This installs shared-schemas from the sibling directory.
uv pip install -e .[all] -e ../shared-schemas
```

---

_Licensed under the MIT License._
