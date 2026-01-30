import os
import sys
import subprocess
from pathlib import Path
from .turingdb import TuringDB, TuringDBException

try:
    from ._version import version as __version__
except ImportError:
    __version__ = "unknown"

__all__ = ["TuringDB", "TuringDBException", "__version__"]

def get_executable_path():
    """Get the path to the compiled C++ executable."""
    import site
    import importlib.util

    # Define possible executable names
    executable_name = "turingdb"  # Adjust this to match your actual binary name

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

    # 2. Use turingdb in the local build directory
    try:
        # Get the installed package location
        spec = importlib.util.find_spec("turingdb")  # Your actual module name
        if spec and spec.origin:
            installed_module_dir = Path(spec.origin).parent
            project_dir = installed_module_dir.parent.parent
            # Build directory locations
            search_locations.append(project_dir / "build/turing_install/bin/")
            search_locations.append(project_dir / "build/tools/turingdb/")
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
        # We're in a virtual environment
        venv_site = Path(sys.prefix) / "lib" / f"python{sys.version_info.major}.{sys.version_info.minor}" / "site-packages"
        search_locations.append(venv_site / "turingdb")

    # Search all locations
    for location in search_locations:
        if not location.exists():
            continue

        executable_path = location / executable_name
        if executable_path.exists() and os.access(executable_path, os.X_OK):
            return str(executable_path)


    print('Search directories:')
    for location in search_locations:
        print(location)

    raise FileNotFoundError(f"Executable '{executable_name}' not found in any expected location")


def main():
    """Entry point for the console script."""
    try:
        executable_path = get_executable_path()

        # Execute the C++ binary directly, replacing the current process
        os.execv(executable_path, [executable_path] + sys.argv[1:])

    except FileNotFoundError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}", file=sys.stderr)
        # Fallback to subprocess if execv fails
        try:
            executable_path = get_executable_path()
            result = subprocess.run([executable_path] + sys.argv[1:])
            sys.exit(result.returncode)
        except:
            sys.exit(1)
