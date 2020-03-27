"""`pigz-python` lives on `GitHub <https://github.com/nix7drummer88/pigz-python>`_."""
from os import path

from setuptools import find_packages, setup

from pigz_python import __version__

this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, "README.md"), encoding="utf-8") as f:
    long_description = f.read()


setup(
    name="pigz-python",
    version=__version__,
    author="Ben Guise",
    author_email="bguise135@gmail.com",
    maintainer="Ben Guise",
    maintainer_email="bguise135@gmail.com",
    description="A pure Python implementation of the pigz utility.",
    license="MIT",
    keywords="zip gzip compression",
    url="https://github.com/nix7drummer88/pigz-python",
    packages=find_packages(),
    long_description=long_description,
    long_description_content_type="text/markdown",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: End Users/Desktop",
        "Intended Audience :: Developers",
        "Intended Audience :: System Administrators",
        "Topic :: Utilities",
    ],
)
