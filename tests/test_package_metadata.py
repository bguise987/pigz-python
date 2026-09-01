"""
Tests for the package's import-time version lookup.

pigz_python/__init__.py reads its version from installed distribution
metadata, which does not exist in a checkout that was never installed. That
used to raise PackageNotFoundError and take the whole package down with it, so
these tests cover the uninstalled path explicitly. Merely importing the package
here would not catch a regression, because the test environment always has the
metadata available.
"""

import importlib
from importlib.metadata import PackageNotFoundError
from unittest.mock import patch

import pigz_python


def test_version_is_a_non_empty_string():
    """The installed package exposes a usable __version__."""
    assert isinstance(pigz_python.__version__, str)
    assert pigz_python.__version__


def test_public_api_is_exported():
    """Importing the package is enough to reach the documented entry points."""
    assert callable(pigz_python.compress_file)
    assert callable(pigz_python.PigzFile)


def test_import_succeeds_without_distribution_metadata():
    """
    A source checkout that was never installed must still import.

    Reloading under a patched lookup reproduces what a fresh clone sees, since
    the real environment always has metadata to find.
    """
    try:
        missing = PackageNotFoundError("pigz-python")
        with patch("importlib.metadata.version", side_effect=missing):
            reloaded = importlib.reload(pigz_python)

            assert isinstance(reloaded.__version__, str)
            assert reloaded.__version__
            assert callable(reloaded.compress_file)
    finally:
        # Restore the real version for anything that runs after this.
        importlib.reload(pigz_python)
