"""
Functions and classes to speed up compression of files by utilizing
multiple cores on a system.
"""
import gzip
import os
import shutil
from multiprocessing.dummy import Pool
from threading import Thread

CPU_COUNT = os.cpu_count()
DEFAULT_BLOCK_SIZE_KB = 128

# 1 is fastest but worst, 9 is slowest but best
GZIP_COMPRESS_OPTIONS = list(range(1, 9 + 1))


class PigzFile:
    def __init__(
        self,
        compression_target,
        keep=True,
        compresslevel=9,
        blocksize=DEFAULT_BLOCK_SIZE_KB,
        recursive=True,
        workers=CPU_COUNT,
    ):
        """
        Take in a file or directory and gzip using multiple system cores.
        """
        self.compression_target = compression_target
        self.keep = keep
        self.blocksize = blocksize
        self.recursive = recursive

        if recursive or os.path.isdir(compression_target):
            raise NotImplementedError

        # Setup the system threads for compression
        self.pool = Pool(processes=workers)
        # Setup read thread
        self.read_thread = Thread(target=read_file, args=(compression_target, blocksize * 1000))
        # Setup write thread
        self.write_thread = Thread(target=write_file, args=(compression_target))

        self.process_compression_target()

    def process_compression_target(self):
        """
        Read in the file(s) in chunks.
        Process those chunks.
        Write the resulting file out.
        """

    def read_file(self, filename, blocksize_bytes):
        """
        Read {filename} in {blocksize} chunks.
        """
        chunk_count = 0
        with open(filename, "rb") as input_file:
            while True:
                chunk = input_file.read(blocksize_bytes)
                # Break out of the loop if we didn't read anything
                if not chunk:
                    break
                chunk_count += 1
                # TODO: Apply this chunk to the pool
        print(f"Read the whole file, chunk count is: {chunk_count}")

    def calculate_crc(self):
        """
        Calculate the checksum for the
        """

    def write_file(self, filename):
        """
        Write {filename} to disk.
        """
        pass

    def clean_up(self):
        """
        Delete original file if user doesn't want to keep it.
        Clean up the processing pool.
        """
        if not self.keep:
            if os.path.isdir(self.compression_target):
                shutil.rmtree(self.compression_target)
            else:
                os.remove(self.compression_target)

        self.pool.close()
        self.pool.join()


def main():
    from argparse import ArgumentParser


if __name__ == "__main__":
    main()
