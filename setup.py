#!/usr/bin/env python3
from setuptools import setup, find_packages
from setuptools.command.develop import develop
from setuptools.command.egg_info import egg_info
from setuptools.command.bdist_wheel import bdist_wheel as _bdist_wheel

from build_backend import _ensure_executables_built, _get_project_root


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
        _ensure_executables_built()
        super().run()


class bdist_wheel(_bdist_wheel):
    """Custom bdist_wheel that marks the wheel as platform-specific."""

    def finalize_options(self):
        super().finalize_options()
        self.root_is_pure = False


setup(
    name="turingdb",
    packages=find_packages(where="python"),
    package_dir={"": "python"},
    include_package_data=True,
    package_data={"turingdb": ["bin/*", "lib/turingdb/extensions/*.so", "lib/turingdb/extensions/*.dylib"]},
    has_ext_modules=lambda: True,
    cmdclass={
        "egg_info": BuildDirEggInfo,
        "develop": CMakeDevelop,
        "bdist_wheel": bdist_wheel,
    },
    python_requires=">=3.10",
    # Dependencies are defined in pyproject.toml
    entry_points={"console_scripts": [
        "turingdb=turingdb:main",
        "turing-parquet=turingdb:main_parquet",
    ]},
)
