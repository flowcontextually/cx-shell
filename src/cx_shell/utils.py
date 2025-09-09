import sys
from pathlib import Path
from .engine.connector.config import CX_HOME


def get_pkg_root() -> Path:
    """
    Gets the root directory of the cx_shell package. This works correctly
    whether running from source or as a frozen PyInstaller executable.
    """
    if getattr(sys, "frozen", False):
        # In the bundle, the package root is the temporary _MEIPASS directory.
        # Our .spec file places the 'cx_shell' directory inside it.
        return Path(sys._MEIPASS) / "cx_shell"
    else:
        # In development, the package root is the parent of this file.
        # i.e., .../src/cx_shell/
        return Path(__file__).parent


def get_assets_root() -> Path:
    """
    Gets the root directory of the bundled 'assets'.
    """
    return get_pkg_root() / "assets"


# This existing helper can remain for user-facing path expansion.
def resolve_path(path_str: str) -> Path:
    """
    Expands common path patterns into absolute paths.
    - `~` is expanded to the user's home directory.
    - `app-asset:` is expanded relative to the CX_HOME directory.
    """
    # --- THIS IS THE FIX ---
    if path_str.startswith("app-asset:"):
        relative_path = path_str.split(":", 1)[1]
        return (CX_HOME / relative_path).resolve()
    # --- END FIX ---

    return Path(path_str).expanduser().resolve()
