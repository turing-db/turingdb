import os
import sys
import subprocess
from pathlib import Path
from .turing_client import TuringClient
from .turing_db import TuringDB
from .http_client import HTTPClient
from .binary_client import BinaryClient
from .exceptions import TuringDBException

try:
    from ._version import version as __version__
except ImportError:
    __version__ = "unknown"

__all__ = ["TuringClient", "TuringDB", "HTTPClient", "BinaryClient", "TuringDBException", "__version__"]

def get_executable_path(executable_name="turingdb"):
    """Get the path to a compiled C++ executable shipped with the package."""
    import site
    import importlib.util

    # Search locations in order of preference
    search_locations = []

    # 1. Check within the installed package (for wheel installs)
    try:
        spec = importlib.util.find_spec("turingdb")
        if spec and spec.origin:
            installed_module_dir = Path(spec.origin).parent
            search_locations.append(installed_module_dir / "bin")
    except:
        pass

    # 2. Local build directory (editable installs)
    try:
        spec = importlib.util.find_spec("turingdb")
        if spec and spec.origin:
            installed_module_dir = Path(spec.origin).parent
            project_dir = installed_module_dir.parent.parent
            search_locations.append(project_dir / "build/turing_install/bin/")
            search_locations.append(project_dir / "build/tools/turingdb/")
            search_locations.append(project_dir / "build/tools/turing-parquet/")
    except:
        pass

    # 3. User site-packages
    try:
        user_site = site.getusersitepackages()
        if user_site:
            user_site_path = Path(user_site) / "bin"
            search_locations.append(user_site_path)
    except:
        pass

    # 4. Site-packages locations
    for site_path in site.getsitepackages():
        site_bin_path = Path(site_path) / "bin"
        search_locations.append(site_bin_path)

    # 5. Virtual environment site-packages (if we're in a venv)
    if hasattr(sys, 'prefix') and sys.prefix != sys.base_prefix:
        venv_site = Path(sys.prefix) / "lib" / f"python{sys.version_info.major}.{sys.version_info.minor}" / "site-packages"
        search_locations.append(venv_site / "turingdb" / "bin")

    # Search all locations
    for location in search_locations:
        if not location.exists():
            continue

        executable_path = location / executable_name
        if executable_path.is_file() and os.access(executable_path, os.X_OK):
            return str(executable_path)


    print('Search directories:')
    for location in search_locations:
        print(location)

    raise FileNotFoundError(f"Executable '{executable_name}' not found in any expected location")


def _run_executable(executable_name):
    """Locate and exec a shipped C++ executable, replacing the current process."""
    try:
        executable_path = get_executable_path(executable_name)
        os.execv(executable_path, [executable_path] + sys.argv[1:])

    except FileNotFoundError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}", file=sys.stderr)
        try:
            executable_path = get_executable_path(executable_name)
            result = subprocess.run([executable_path] + sys.argv[1:])
            sys.exit(result.returncode)
        except:
            sys.exit(1)


def main():
    """Entry point for the turingdb console script."""
    _run_executable("turingdb")


def main_parquet():
    """Entry point for the turing-parquet console script."""
    _run_executable("turing-parquet")
