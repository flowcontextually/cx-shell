import sys
from pathlib import Path


def get_asset_path(relative_path: str) -> Path:
    """
    Gets the absolute path to a bundled asset, working correctly whether
    running from source or as a frozen PyInstaller executable.
    """
    if getattr(sys, "frozen", False):
        # We are running in a bundle
        base_path = Path(sys._MEIPASS).joinpath("cx_shell")
    else:
        # We are running in a normal Python environment
        base_path = Path(__file__).parent

    return (base_path / "assets" / relative_path).resolve()
