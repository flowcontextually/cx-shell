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


# def resolve_path(path_str: str) -> Path:
#     """
#     Expands common path patterns into absolute paths.
#     - `~` is expanded to the user's home directory.
#     - `app-asset:` is expanded relative to the CX_HOME directory.
#     """
#     if path_str.startswith("app-asset:"):
#         relative_path = path_str.split(":", 1)[1]
#         return (CX_HOME / relative_path).resolve()
#     return Path(path_str).expanduser().resolve()


# def resolve_path(path_str: str) -> Path:
#     """
#     Expands common path patterns into absolute paths.
#     - `~` is expanded to the user's home directory.
#     - `app-asset:` is expanded relative to the CX_HOME directory.
#     - `file://` URIs are correctly parsed to an absolute path.
#     """
#     if path_str.startswith("app-asset:"):
#         relative_path = path_str.split(":", 1)[1]
#         return (CX_HOME / relative_path).resolve()

#     # --- THIS IS THE NEW, CRITICAL LOGIC ---
#     if path_str.startswith("file://"):
#         # Strip the scheme and return an absolute path object
#         return Path(path_str[7:]).resolve()
#     # --- END NEW LOGIC ---


#     # Fallback for standard paths like `~/foo` or `./bar`
#     return Path(path_str).expanduser().resolve()\
def resolve_path(path_str: str) -> Path:
    """
    Expands common path patterns into absolute paths.
    - `~` is expanded to the user's home directory.
    - `app-asset:` is expanded relative to the CX_HOME directory.
    - `file:` URIs are correctly parsed to an absolute path.
    """
    if path_str.startswith("app-asset:"):
        relative_path = path_str.split(":", 1)[1]
        return (CX_HOME / relative_path).resolve()

    # --- THIS IS THE DEFINITIVE FIX ---
    # Handle the "file:" URI scheme, robustly handling single or triple slashes.
    if path_str.startswith("file:"):
        # Find the start of the actual path after the scheme
        path_part = path_str.split(":", 1)[1]
        # Strip any leading slashes to get the absolute path
        clean_path = path_part.lstrip("/")
        # The path must start with a slash to be absolute on Unix-like systems.
        # On Windows, it might start with a drive letter.
        # Path() handles this correctly if the path is absolute.
        return Path(f"/{clean_path}").resolve()
    # --- END DEFINITIVE FIX ---

    # Fallback for standard paths like `~/foo` or `./bar`
    return Path(path_str).expanduser().resolve()
