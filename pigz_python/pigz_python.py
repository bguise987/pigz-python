"""
Functions and classes to speed up compression of files by utilizing
multiple cores on a system.
"""
import gzip
import os
import shutil
import time
from multiprocessing.dummy import Pool
from queue import PriorityQueue
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

        self.output_file = None
        self.output_filename = None

        # This is how we know if we're done reading, compressing, & writing the file
        self.last_chunk = -1

        self.chunk_queue = PriorityQueue()

        if recursive or os.path.isdir(compression_target):
            raise NotImplementedError

        # Setup the system threads for compression
        self.pool = Pool(processes=workers)
        # Setup read thread
        self.read_thread = Thread(
            target=self.read_file, args=(compression_target, blocksize * 1000)
        )
        # Setup write thread
        self.write_thread = Thread(target=self.write_file)

        self.process_compression_target()

    def process_compression_target(self):
        """
        Read in the file(s) in chunks.
        Process those chunks.
        Write the resulting file out.
        """
        self.setup_output_file()
        # Start the write thread first so it's ready to accept data
        self.write_thread.start()
        # Start the read thread
        self.read_thread.start()

    def setup_output_file(self):
        """
        Determine output filename.
        Setup the output file object.
        """
        base = os.path.basename(self.compression_target)
        self.output_filename = os.path.splitext(base)[0]
        self.output_file = open(self.output_filename, "wb")

        self.write_output_header()

    def write_output_header(self):
        """
        Write gzip header to file
        """
        # TODO: Write out data header

    def read_file(self, filename, blocksize_bytes):
        """
        Read {filename} in {blocksize} chunks.
        This method is run on the read thread.
        """
        chunk_num = 0
        with open(filename, "rb") as input_file:
            while True:
                chunk = input_file.read(blocksize_bytes)
                # Break out of the loop if we didn't read anything
                if not chunk:
                    self.last_chunk = chunk_num
                    break

                # TODO: Apply this chunk to the pool
                chunk_num += 1

    def process_chunk(self, chunk: bytes):
        """
        Overall method to handle the chunk and pass it back to the write thread.
        This method is run on the pool.
        """
        # TODO: Is this the right order?
        self.calculate_chunk_check(chunk)
        self.compress_chunk(chunk)

    def calculate_chunk_check(self, chunk: bytes):
        """
        Calculate the check value for the chunk.
        """
        # TODO: Calculate the chunk check

    def compress_chunk(self, chunk: bytes):
        """
        Compress the chunk.
        """
        # TODO: Do the compression work with zlib

    def write_file(self):
        """
        Write compressed data to disk.
        Read chunks off of the priority queue.
        Priority is the chunk number, so we can keep track of which chunk to get next.
        """
        next_chunk_num = 0
        while True:
            if not self.chunk_queue.empty():
                chunk_num, chunk = self.chunk_queue.get()

                if chunk_num != next_chunk_num:
                    # If this isn't the next chunk we're looking for, place it back on the queue and sleep
                    self.chunk_queue.put((chunk_num, chunk))
                    time.sleep(0.5)
                else:
                    # Write chunk to file, advance next chunk we're looking for
                    self.output_file.write(chunk)
                    # If this was the last chunk, we can break the loop and close the file
                    if chunk_num == self.last_chunk:
                        break
                    next_chunk_num += 1
            else:
                # If the queue is empty, we're likely waiting for data.
                time.sleep(0.5)

        # Loop breaks out if we've received the final chunk
        self.clean_up()

    def clean_up(self):
        """
        Close the output file.
        Delete original file or directory if the user doesn't want to keep it.
        Clean up the processing pool.
        """
        self.write_file_trailer()

        self.output_file.close()

        self.handle_keep()

        self.close_workers()

    def write_file_trailer(self):
        """
        Write the trailer for the compressed data.
        """
        # TODO: Write trailer

    def handle_keep(self):
        """
        Delete the file / folder if the user so desires.
        """
        if not self.keep:
            if os.path.isdir(self.compression_target):
                shutil.rmtree(self.compression_target)
            else:
                os.remove(self.compression_target)

    def close_workers(self):
        """
        Stop threads and close pool.
        """
        self.read_thread.join()
        self.write_thread.join()
        self.pool.close()
        self.pool.join()


def main():
    from argparse import ArgumentParser


if __name__ == "__main__":
    main()
