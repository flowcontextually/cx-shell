# /home/dpwanjala/repositories/cx-shell/src/cx_shell/utils.py

import sys
from pathlib import Path


def resolve_path(path_str: str) -> Path:
    """
    Expands the user's home directory ('~') and resolves the path.
    """
    return Path(path_str).expanduser().resolve()


def get_asset_path(relative_path: str) -> Path:
    """
    Gets the absolute path to a bundled asset, working correctly whether
    running from source or as a frozen PyInstaller executable.

    `relative_path` should be relative to the `src/cx_shell` directory.
    """
    if getattr(sys, "frozen", False):
        # We are running in a PyInstaller bundle. The root is `sys._MEIPASS`.
        # Our spec file places all assets inside a `cx_shell` directory
        # within the bundle, so we add that to the base path.
        base_path = Path(sys._MEIPASS) / "cx_shell"
    else:
        # We are running in a normal Python environment.
        # The base is the `src/cx_shell` directory, which is the parent
        # of this `utils.py` file.
        base_path = Path(__file__).parent

    return (base_path / relative_path).resolve()
