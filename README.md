# The Contextual Shell (`cx`) by Contextually

<!-- <p align="center"> -->
  <!-- TODO: Record a GIF of the "5-Minute Tutorial" steps and replace this placeholder -->
  <!-- <img src="https://raw.githubusercontent.com/flowcontextually/docs/main/docs/assets/cx-shell-demo-placeholder.gif" alt="Contextually Shell Demo">
</p> -->

Welcome to the **Contextual Shell**, a declarative, multi-stage automation platform for modern data and operations teams. `cx` is the core command-line application for the **Contextually** platform, a universal computational fabric where work retains its full context as it flows between different tools, people, and AI agents.

---

## 🚀 Quick Start: The 5-Minute Tutorial

Get your first "win" in under five minutes. This tutorial will guide you through installing `cx`, initializing a sample project, and making your first dynamic API call to GitHub.

### 1. Installation

Download and install the latest pre-compiled binary for your operating system from our [**GitHub Releases page**](https://github.com/flowcontextually/cx-shell/releases).

#### Linux

```bash
# This script downloads the latest Linux binary, extracts it, and moves it to your path.
curl -sL https://github.com/flowcontextually/cx-shell/releases/download/v0.6.0/cx-v0.6.0-linux-x86_64.tar.gz | tar -xz
sudo mv cx /usr/local/bin/
cx --version
```

#### macOS (Intel)

```bash
# This script downloads the latest macOS (Intel) binary, extracts it, and moves it to your path.
curl -sL https://github.com/flowcontextually/cx-shell/releases/download/v0.6.0/cx-v0.6.0-macos-x86_64.tar.gz | tar -xz
sudo mv cx /usr/local/bin/
cx --version
```

#### macOS (Apple Silicon)

> **Warning:** Apple Silicon (M1/M2/M3)  
> A native Apple Silicon build is not yet available. You can run the Intel version via Rosetta 2 using the macOS (Intel) instructions.

#### Windows (PowerShell)

```powershell
# This script downloads the latest Windows binary and unzips it.
$url = "https://github.com/flowcontextually/cx-shell/releases/download/v0.6.0/cx-v0.6.0-windows-amd64.zip"
$output = "cx.zip"
Invoke-WebRequest -Uri $url -OutFile $output
Expand-Archive -Path $output -DestinationPath .

# You should now have `cx.exe` in the current directory.
# For system-wide access, move `cx.exe` to a directory in your system's PATH.
./cx.exe --version
```

### 2. Initialize Your Environment

The `cx init` command creates the necessary configuration files and a sample "GitHub API" blueprint inside your home directory at `~/.cx/`.

```bash
cx init
```

### 3. Run Your First Command!

Now, start the interactive shell and connect to the sample API.

```bash
# 1. Start the interactive shell
cx
```

Inside the shell, run the following commands:

```
# 2. Connect to the public GitHub API with the alias 'gh'
cx> connect user:github --as gh

# 3. Run your first blueprint-driven command!
cx> gh.getUser(username="torvalds")
```

**Congratulations!** You've just executed a dynamic, validated API call using a pre-compiled blueprint.

## 📚 Documentation

For full documentation, tutorials, and architectural deep dives, please visit our [**official documentation site**](https://flowcontextually.github.io/docs/).

## 🤝 Contributing

We welcome contributions of all kinds! The most valuable way to contribute is by adding new integrations to our public [**Blueprint Registry**](https://github.com/flowcontextually/blueprints).

### Setting up a Development Environment

If you'd like to contribute to the core `cx-shell` application, you'll need the following:

- Python 3.12+
- `uv` (installed via `pip install uv`)

The setup is now streamlined, and all dependencies are handled automatically.

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
