"""Metadata about the Pigz Python package"""

from importlib.metadata import PackageNotFoundError, version

from pigz_python.pigz_python import PigzFile, compress_file  # noqa

try:
    __version__ = version("pigz-python")
except PackageNotFoundError:
    # No distribution metadata, so this is a source checkout that was never
    # installed. The version lives in pyproject.toml and is only readable here
    # once the package has been built, so importing must not depend on it.
    __version__ = "unknown"
