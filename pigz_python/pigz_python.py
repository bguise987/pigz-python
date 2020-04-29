"""
Functions and classes to speed up compression of files by utilizing
multiple cores on a system.
"""
import os
import shutil
import sys
import time
from multiprocessing.dummy import Pool
from queue import PriorityQueue
from threading import Thread
from zlib import Z_SYNC_FLUSH, compressobj, crc32

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
        self.compression_level = compresslevel
        self.blocksize = blocksize * 1000
        self.recursive = recursive

        self.mtime = self._determine_mtime()

        self.output_file = None
        self.output_filename = None

        # This is how we know if we're done reading, compressing, & writing the file
        self.last_chunk = -1

        self.chunk_queue = PriorityQueue()

        if os.path.isdir(compression_target):
            raise NotImplementedError

        # Setup the system threads for compression
        self.pool = Pool(processes=workers)
        # Setup read thread
        self.read_thread = Thread(target=self.read_file)
        # Setup write thread
        self.write_thread = Thread(target=self.write_file)

        self.process_compression_target()

    def _determine_mtime(self):
        """
        Determine MTIME to write out in Unix format (seconds since Unix epoch).
        From http://www.zlib.org/rfc-gzip.html#header-trailer:
        If the compressed data did not come from a file, MTIME is set to the time at which compression started.
        MTIME = 0 means no time stamp is available.
        """
        try:
            return os.stat(self.compression_target).mtime
        except Exception:
            return 0

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
        self.output_filename = base + ".gz"
        self.output_file = open(self.output_filename, "wb")

        self.write_output_header()

    def write_output_header(self):
        """
        Write gzip header to file
        """
        # TODO: Write out data header
        # See line 1027 of pigz.c
        # else {                          // gzip
        #     len = put(g.outd, # output file descriptor
        #         1, (val_t)31,
        #         1, (val_t)139,
        #         1, (val_t)8,            // deflate
        #         1, (val_t)(g.name != NULL ? 8 : 0),
        #         4, (val_t)g.mtime,
        #         1, (val_t)(g.level >= 9 ? 2 : g.level == 1 ? 4 : 0),
        #         1, (val_t)3,            // unix
        #         0);
        #     if (g.name != NULL)
        #         len += writen(g.outd, g.name, strlen(g.name) + 1);
        # }

        # See this also: http://www.zlib.org/rfc-gzip.html#header-trailer

        # Write ID (IDentification) ID 1, then ID 2. These denote the file as being gzip format.
        self.output_file.write((0x1F).to_bytes(1, sys.byteorder))
        self.output_file.write((0x8B).to_bytes(1, sys.byteorder))
        # Write the CM (compression method)
        self.output_file.write((self.compression_level).to_bytes(1, sys.byteorder))
        # Write MTIME (Modification time)
        self.output_file.write((self.mtime).to_bytes(4, sys.byteorder))
        # Write XFL (eXtra FLags)
        extra_flags = self._determine_extra_flags(self.compression_level)
        self.output_file.write((extra_flags).to_bytes(1, sys.byteorder))
        # Write OS
        os_number = self._determine_operating_system()
        self.output_file.write((os_number).to_bytes(1, sys.byteorder))

    def _determine_extra_flags(self, compression_level):
        """
        Determine the XFL or eXtra FLags value based on compression level.
        Note this is copied from the pigz implementation.
        """
        return 2 if compression_level >= 9 else 4 if compression_level == 1 else 0

    def _determine_operating_system(self):
        """
        Return appropriate number based on OS format.
        0 - FAT filesystem (MS-DOS, OS/2, NT/Win32)
        1 - Amiga
        2 - VMS (or OpenVMS)
        3 - Unix
        4 - VM/CMS
        5 - Atari TOS
        6 - HPFS filesystem (OS/2, NT)
        7 - Macintosh
        8 - Z-System
        9 - CP/M
        10 - TOPS-20
        11 - NTFS filesystem (NT)
        12 - QDOS
        13 - Acorn RISCOS
        255 - unknown
        """
        if sys.platform.startswith(("freebsd", "linux", "aix")):
            return 3
        elif sys.platform.startswith(("darwin")):
            return 7
        elif sys.platform.startswith(("win32")):
            return 0

        return 255

    def read_file(self):
        """
        Read {filename} in {blocksize} chunks.
        This method is run on the read thread.
        """
        chunk_num = 0
        with open(self.compression_target, "rb") as input_file:
            while True:
                chunk = input_file.read(self.blocksize)
                # Break out of the loop if we didn't read anything
                if not chunk:
                    # Since we previously advanced chunk_num counter before we knew we reached EOF, decrement 1
                    self.last_chunk = chunk_num - 1
                    break

                # TODO: Apply this chunk to the pool
                self.pool.apply_async(self.process_chunk, (chunk_num, chunk))
                chunk_num += 1

    def process_chunk(self, chunk_num: int, chunk: bytes):
        """
        Overall method to handle the chunk and pass it back to the write thread.
        This method is run on the pool.
        """
        chunk_check = self.calculate_chunk_check(chunk)
        compressed_chunk = self.compress_chunk(chunk)
        self.chunk_queue.put((chunk_num, compressed_chunk, chunk_check))

    def calculate_chunk_check(self, chunk: bytes):
        """
        Calculate the check value for the chunk.
        """
        # Note: See crc32 documentation - might be able to utilize it to calculate the running checksum
        return crc32(chunk)

    def compress_chunk(self, chunk: bytes):
        """
        Compress the chunk.
        """
        compressor = compressobj(self.compression_level)
        compressed_data = compressor.compress(chunk)
        compressed_data += compressor.flush(Z_SYNC_FLUSH)
        return compressed_data

    def write_file(self):
        """
        Write compressed data to disk.
        Read chunks off of the priority queue.
        Priority is the chunk number, so we can keep track of which chunk to get next.
        This is run from the write thread.
        """
        next_chunk_num = 0
        while True:
            if not self.chunk_queue.empty():
                chunk_num, compressed_chunk, chunk_check = self.chunk_queue.get()

                if chunk_num != next_chunk_num:
                    # If this isn't the next chunk we're looking for, place it back on the queue and sleep
                    self.chunk_queue.put((chunk_num, compressed_chunk, chunk_check))
                    time.sleep(0.5)
                else:
                    # Write chunk to file, advance next chunk we're looking for
                    self.output_file.write(compressed_chunk)
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
        # Write CRC32
        # Write ISIZE

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
        self.pool.terminate()
        self.pool.join()


def main():
    """ Run pigz Python as a standalone module """
    # from argparse import ArgumentParser


if __name__ == "__main__":
    main()
