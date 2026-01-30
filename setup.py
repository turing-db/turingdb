#!/usr/bin/env python3
import os
import shutil
import subprocess
import sys
from pathlib import Path
from setuptools import setup, find_packages
from setuptools.command.develop import develop
from setuptools.command.egg_info import egg_info
from setuptools.command.bdist_wheel import bdist_wheel as _bdist_wheel


def _get_project_root() -> Path:
    """Get the project root directory."""
    return Path(__file__).parent.resolve()


def _get_build_executable() -> Path:
    """Get the path to the executable in the build directory."""
    turing_home = os.environ.get("TURING_HOME")
    if turing_home:
        return Path(turing_home) / "bin" / "turingdb"
    return _get_project_root() / "build" / "turing_install" / "bin" / "turingdb"


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
    """Ensure the executable is built, building if necessary."""
    exe_path = _get_build_executable()

    if exe_path.exists():
        return exe_path

    cmake_file = _get_project_root() / "CMakeLists.txt"
    if not cmake_file.exists():
        raise RuntimeError(
            "Cannot build turingdb: CMakeLists.txt not found and no pre-built "
            "executable exists. Please build from the source repository:\n"
            "  cd build && cmake .. && make -j8 && make install\n"
            "Then run the build again."
        )

    _run_cmake_build()

    if not exe_path.exists():
        raise RuntimeError(f"Build completed but executable not found at {exe_path}")

    return exe_path


class BuildDirEggInfo(egg_info):
    """Custom egg_info that writes to build/ instead of python/."""
    def initialize_options(self):
        super().initialize_options()
        build_dir = _get_project_root() / "build"
        build_dir.mkdir(parents=True, exist_ok=True)
        self.egg_base = str(build_dir)


class CMakeDevelop(develop):
    """Custom develop command that ensures the executable is built."""
    def run(self):
        _ensure_executable_built()
        super().run()


class bdist_wheel(_bdist_wheel):
    """Custom bdist_wheel that allows overriding the Python tag via environment variable."""

    def finalize_options(self):
        super().finalize_options()
        # Allow overriding python_tag via PYTHON_TAG environment variable
        python_tag = os.environ.get('PYTHON_TAG')
        if python_tag:
            self.python_tag = python_tag
        else:
            # Default to current Python version (cpXY format)
            self.python_tag = f'cp{sys.version_info.major}{sys.version_info.minor}'


setup(
    name="turingdb",
    packages=find_packages(where="python"),
    package_dir={"": "python"},
    include_package_data=True,
    package_data={"turingdb": ["bin/*"]},
    cmdclass={
        "egg_info": BuildDirEggInfo,
        "develop": CMakeDevelop,
        "bdist_wheel": bdist_wheel,
    },
    python_requires=">=3.10",
    # Dependencies are defined in pyproject.toml
    entry_points={"console_scripts": ["turingdb=turingdb:main"]},
)
