# /home/dpwanjala/repositories/cx-shell/src/cx_shell/utils.py
import sys
from pathlib import Path
import os

# --- Centralized Path Constant ---
# This is now the single source of truth for the CX_HOME path.
CX_HOME = Path(os.getenv("CX_HOME", Path.home() / ".cx"))


def get_pkg_root() -> Path:
    """
    Gets the root directory of the cx_shell package. This works correctly
    whether running from source or as a frozen PyInstaller executable.
    """
    if getattr(sys, "frozen", False):
        return Path(sys._MEIPASS) / "cx_shell"
    else:
        return Path(__file__).parent


def get_assets_root() -> Path:
    """Gets the root directory of the bundled 'assets'."""
    return get_pkg_root() / "assets"


def resolve_path(path_str: str) -> Path:
    """
    Expands common path patterns into absolute paths.
    - `~` is expanded to the user's home directory.
    - `app-asset:` is expanded relative to the CX_HOME directory.
    """
    if path_str.startswith("app-asset:"):
        relative_path = path_str.split(":", 1)[1]
        return (CX_HOME / relative_path).resolve()
    return Path(path_str).expanduser().resolve()
