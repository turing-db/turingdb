"""Custom build backend for turingdb that builds wheels with the C++ binary."""

import os
import shlex
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


# Binaries shipped in the wheel under turingdb/bin/.
_SHIPPED_EXECUTABLES = ("turingdb", "turing-parquet")


def _get_project_root() -> Path:
    """Get the project root directory."""
    return Path(__file__).parent.resolve()


def _get_build_bin_dir() -> Path:
    """Get the directory holding built executables."""
    turing_home = os.environ.get("TURING_HOME")
    if turing_home:
        return Path(turing_home) / "bin"
    return _get_project_root() / "build" / "turing_install" / "bin"


def _get_package_bin_dir() -> Path:
    """Get the bin directory inside the Python package (used by sdist)."""
    return _get_project_root() / "python" / "turingdb" / "bin"


def _get_build_executable(name: str = "turingdb") -> Path:
    """Get the path to a built executable."""
    return _get_build_bin_dir() / name


def _get_package_executable(name: str = "turingdb") -> Path:
    """Get the path to an executable in the package directory (used by sdist)."""
    return _get_package_bin_dir() / name


def _get_build_extensions_dir() -> Path:
    """Get the path to built extensions in the build directory."""
    turing_home = os.environ.get("TURING_HOME")
    if turing_home:
        return Path(turing_home) / "lib" / "turingdb" / "extensions"
    return _get_project_root() / "build" / "lib" / "turingdb" / "extensions"


def _get_package_extensions_dir() -> Path:
    """Get the path to extensions in the package directory."""
    return _get_project_root() / "python" / "turingdb" / "lib" / "turingdb" / "extensions"


def _strip_binary(path: Path) -> None:
    """Strip debug symbols from a built binary or shared library."""
    strip = shutil.which("strip")
    if strip is None:
        return
    subprocess.check_call([strip, str(path)])


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
    ]

    # Read CMAKE_ARGS from environment (used by CI for compiler paths)
    extra_cmake_args = os.environ.get("CMAKE_ARGS", "")
    if extra_cmake_args:
        cmake_args.extend(shlex.split(extra_cmake_args))

    cmake_args.append(str(source_dir))

    subprocess.check_call(cmake_args, cwd=str(build_dir))
    subprocess.check_call(["make", f"-j{os.cpu_count() or 4}"], cwd=str(build_dir))
    subprocess.check_call(["make", "install"], cwd=str(build_dir))


def _resolve_executable(name: str) -> Path:
    """Find a shipped executable, in package dir (sdist) or build dir."""
    pkg_exe = _get_package_executable(name)
    if pkg_exe.exists():
        return pkg_exe
    return _get_build_executable(name)


def _ensure_executables_built() -> dict[str, Path]:
    """Ensure every shipped executable is built. Returns name -> source path."""
    resolved = {name: _resolve_executable(name) for name in _SHIPPED_EXECUTABLES}
    if all(path.exists() for path in resolved.values()):
        return resolved

    cmake_file = _get_project_root() / "CMakeLists.txt"
    if not cmake_file.exists():
        missing = ", ".join(name for name, path in resolved.items() if not path.exists())
        raise RuntimeError(
            f"Cannot build turingdb: CMakeLists.txt not found and pre-built "
            f"executable(s) missing ({missing}). Please build from the source repository:\n"
            "  cd build && cmake .. && make -j8 && make install\n"
            "Then run the wheel build again."
        )

    _run_cmake_build()

    resolved = {name: _resolve_executable(name) for name in _SHIPPED_EXECUTABLES}
    for name, path in resolved.items():
        if not path.exists():
            raise RuntimeError(f"Build completed but executable not found at {path}")

    return resolved


def build_wheel(wheel_directory, config_settings=None, metadata_directory=None):
    """Build a wheel with the C++ binary included."""
    import setuptools.build_meta as backend

    project_root = _get_project_root()
    binary_dir = project_root / "python" / "turingdb" / "_binary"
    local_dir = project_root / "python" / "turingdb" / "_local"
    binary_pre_existing = {
        p.name
        for p in list(binary_dir.glob("_turingproto*.so")) + list(binary_dir.glob("_turingproto*.pyd"))
    }
    local_pre_existing = {
        p.name
        for p in list(local_dir.glob("_turinglocal*.so")) + list(local_dir.glob("_turinglocal*.pyd"))
    }

    # Ensure all shipped executables exist
    exe_paths = _ensure_executables_built()

    bin_dir = _get_package_bin_dir()
    build_lib_dir = project_root / "build" / "lib"
    ext_src_dir = _get_build_extensions_dir()
    ext_dest_dir = _get_package_extensions_dir()

    # Clean up build/lib to avoid including gtest/gmock static libraries
    if build_lib_dir.exists():
        for f in build_lib_dir.glob("*.a"):
            f.unlink()

    # Track which binaries we copied so cleanup removes only those.
    copied_binaries: list[Path] = []
    copied_extensions = False

    try:
        bin_dir.mkdir(parents=True, exist_ok=True)
        for name, src in exe_paths.items():
            dest = bin_dir / name
            if src == dest:
                continue
            shutil.copy2(src, dest)
            os.chmod(dest, 0o755)
            copied_binaries.append(dest)
            _strip_binary(dest)

        # Copy extension shared libraries (.so on Linux, .dylib on macOS)
        if ext_src_dir.exists() and not ext_dest_dir.exists():
            ext_dest_dir.mkdir(parents=True, exist_ok=True)
            copied_extensions = True
            for ext_file in list(ext_src_dir.glob("*.so")) + list(ext_src_dir.glob("*.dylib")):
                dest = ext_dest_dir / ext_file.name
                shutil.copy2(ext_file, dest)
                _strip_binary(dest)

        # Strip native pybind11 modules produced by the cmake build, but leave
        # any pre-existing ones alone (a developer's editable install).
        for native_file in list(binary_dir.glob("_turingproto*.so")) + list(binary_dir.glob("_turingproto*.pyd")):
            if native_file.name not in binary_pre_existing:
                _strip_binary(native_file)
        for native_file in list(local_dir.glob("_turinglocal*.so")) + list(local_dir.glob("_turinglocal*.pyd")):
            if native_file.name not in local_pre_existing:
                _strip_binary(native_file)

        # Build the wheel using setuptools
        wheel_name = backend.build_wheel(
            wheel_directory, config_settings, metadata_directory
        )

        return wheel_name
    finally:
        # Remove only the binaries we copied; leave pre-existing sdist binaries alone.
        for path in copied_binaries:
            if path.exists():
                path.unlink()
        if bin_dir.exists() and not any(bin_dir.iterdir()):
            bin_dir.rmdir()
        if copied_extensions and ext_dest_dir.exists():
            shutil.rmtree(ext_dest_dir)
        # Remove native .so/.pyd files we just produced (don't touch any that pre-existed,
        # which would belong to a developer's editable install).
        if binary_dir.exists():
            for binary_file in list(binary_dir.glob("_turingproto*.so")) + list(binary_dir.glob("_turingproto*.pyd")):
                if binary_file.name not in binary_pre_existing:
                    binary_file.unlink()
        if local_dir.exists():
            for local_file in list(local_dir.glob("_turinglocal*.so")) + list(local_dir.glob("_turinglocal*.pyd")):
                if local_file.name not in local_pre_existing:
                    local_file.unlink()


def build_sdist(sdist_directory, config_settings=None):
    """Build an sdist with the C++ binaries included."""
    import setuptools.build_meta as backend

    # Ensure all shipped executables exist
    exe_paths = _ensure_executables_built()

    bin_dir = _get_package_bin_dir()
    ext_src_dir = _get_build_extensions_dir()
    ext_dest_dir = _get_package_extensions_dir()

    copied_binaries: list[Path] = []
    copied_extensions = False

    try:
        bin_dir.mkdir(parents=True, exist_ok=True)
        for name, src in exe_paths.items():
            dest = bin_dir / name
            if src == dest:
                continue
            shutil.copy2(src, dest)
            os.chmod(dest, 0o755)
            copied_binaries.append(dest)

        # Copy extension shared libraries (.so on Linux, .dylib on macOS)
        if ext_src_dir.exists() and not ext_dest_dir.exists():
            ext_dest_dir.mkdir(parents=True, exist_ok=True)
            copied_extensions = True
            for ext_file in list(ext_src_dir.glob("*.so")) + list(ext_src_dir.glob("*.dylib")):
                shutil.copy2(ext_file, ext_dest_dir / ext_file.name)

        # Build the sdist using setuptools
        sdist_name = backend.build_sdist(sdist_directory, config_settings)

        return sdist_name
    finally:
        for path in copied_binaries:
            if path.exists():
                path.unlink()
        if bin_dir.exists() and not any(bin_dir.iterdir()):
            bin_dir.rmdir()
        if copied_extensions and ext_dest_dir.exists():
            shutil.rmtree(ext_dest_dir)
