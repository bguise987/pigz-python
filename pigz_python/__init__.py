""" Metadata about the Pigz Python package """
from importlib.metadata import version
__version__ = version("pigz-python")

from pigz_python.pigz_python import PigzFile, compress_file  # noqa
