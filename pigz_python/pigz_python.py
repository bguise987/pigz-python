"""
Functions and classes to speed up compression of files by utilizing
multiple cores on a system.
"""
import gzip
import os
from multiprocessing.dummy import Pool

CPU_COUNT = os.cpu_count()
DEFAULT_BLOCK_SIZE_KB = 128

# 1 is fastest but worst, 9 is slowest but best
GZIP_COMPRESS_OPTIONS = list(range(1, 9 + 1))


def gzip(
    filename,
    keep=True,
    compresslevel=9,
    blocksize=DEFAULT_BLOCK_SIZE_KB,
    recursive=True,
    workers=CPU_COUNT,
):
    """
    Take in a file or directory and gzip using multiple system cores.
    """
    # Setup the system threads
    pool = Pool(processes=workers)
    pass


def main():
    from argparse import ArgumentParser


if __name__ == "__main__":
    main()
