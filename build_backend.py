"""Custom build backend for turingdb that builds wheels with the C++ binary."""

import os
import shutil
import subprocess
import tempfile
from pathlib import Path

# Re-export setuptools functions we don't override
from setuptools.build_meta import (
    get_requires_for_build_wheel,
    get_requires_for_build_editable,
    get_requires_for_build_sdist,
    prepare_metadata_for_build_wheel,
    prepare_metadata_for_build_editable,
    build_editable,
)


def _get_project_root() -> Path:
    """Get the project root directory."""
    return Path(__file__).parent.resolve()


def _get_build_executable() -> Path:
    """Get the path to the executable in the build directory."""
    return _get_project_root() / "build" / "turing_install" / "bin" / "turingdb"


def _get_package_executable() -> Path:
    """Get the path to the executable in the package directory (used by sdist)."""
    return _get_project_root() / "python" / "turingdb" / "bin" / "turingdb"


def _run_cmake_build():
    """Run cmake configure, build, and install."""
    source_dir = _get_project_root()
    build_dir = source_dir / "build"
    build_dir.mkdir(parents=True, exist_ok=True)

    cmake_args = [
        "cmake",
        "-G", "Unix Makefiles",
        "-DCMAKE_MAKE_PROGRAM=/usr/bin/make",
        "-DCMAKE_BUILD_TYPE=Release",
        str(source_dir),
    ]
    subprocess.check_call(cmake_args, cwd=str(build_dir))
    subprocess.check_call(["make", f"-j{os.cpu_count() or 4}"], cwd=str(build_dir))
    subprocess.check_call(["make", "install"], cwd=str(build_dir))


def _ensure_executable_built() -> Path:
    """Ensure the executable is built, building if necessary. Returns path to executable."""
    # Check package directory first (for sdist builds)
    pkg_exe = _get_package_executable()
    if pkg_exe.exists():
        return pkg_exe

    # Check build directory
    exe_path = _get_build_executable()
    if exe_path.exists():
        return exe_path

    cmake_file = _get_project_root() / "CMakeLists.txt"
    if not cmake_file.exists():
        raise RuntimeError(
            "Cannot build turingdb: CMakeLists.txt not found and no pre-built "
            "executable exists. Please build from the source repository:\n"
            "  cd build && cmake .. && make -j8 && make install\n"
            "Then run the wheel build again."
        )

    _run_cmake_build()

    if not exe_path.exists():
        raise RuntimeError(f"Build completed but executable not found at {exe_path}")

    return exe_path


def build_wheel(wheel_directory, config_settings=None, metadata_directory=None):
    """Build a wheel with the C++ binary included."""
    import setuptools.build_meta as backend

    # Ensure the executable exists
    exe_path = _ensure_executable_built()

    project_root = _get_project_root()
    bin_dir = project_root / "python" / "turingdb" / "bin"
    dest_exe = bin_dir / "turingdb"
    build_lib_dir = project_root / "build" / "lib"

    # Clean up build/lib to avoid including gtest/gmock static libraries
    if build_lib_dir.exists():
        for f in build_lib_dir.glob("*.a"):
            f.unlink()

    # Check if binary is already in package dir (sdist case)
    binary_already_in_place = exe_path == dest_exe

    try:
        if not binary_already_in_place:
            # Copy binary to package directory temporarily
            bin_dir.mkdir(parents=True, exist_ok=True)
            shutil.copy2(exe_path, dest_exe)
            os.chmod(dest_exe, 0o755)

        # Build the wheel using setuptools
        wheel_name = backend.build_wheel(
            wheel_directory, config_settings, metadata_directory
        )

        return wheel_name
    finally:
        # Clean up - remove the temporary binary from source tree (only if we copied it)
        if not binary_already_in_place and bin_dir.exists():
            shutil.rmtree(bin_dir)


def build_sdist(sdist_directory, config_settings=None):
    """Build an sdist with the C++ binary included."""
    import setuptools.build_meta as backend

    # Ensure the executable exists
    exe_path = _ensure_executable_built()

    project_root = _get_project_root()
    bin_dir = project_root / "python" / "turingdb" / "bin"
    dest_exe = bin_dir / "turingdb"

    # Check if binary is already in package dir
    binary_already_in_place = exe_path == dest_exe

    try:
        if not binary_already_in_place:
            # Copy binary to package directory temporarily
            bin_dir.mkdir(parents=True, exist_ok=True)
            shutil.copy2(exe_path, dest_exe)
            os.chmod(dest_exe, 0o755)

        # Build the sdist using setuptools
        sdist_name = backend.build_sdist(sdist_directory, config_settings)

        return sdist_name
    finally:
        # Clean up - remove the temporary binary from source tree (only if we copied it)
        if not binary_already_in_place and bin_dir.exists():
            shutil.rmtree(bin_dir)
