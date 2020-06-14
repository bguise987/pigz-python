"""
Functions and classes to speed up compression of files by utilizing
multiple cores on a system.
"""
import builtins
import io
import os
import shutil
import sys
import time
import zlib
from multiprocessing.dummy import Pool
from queue import PriorityQueue
from threading import Lock, Thread

CPU_COUNT = os.cpu_count()
DEFAULT_BLOCK_SIZE_KB = 128

# 1 is fastest but worst, 9 is slowest but best
GZIP_COMPRESS_OPTIONS = list(range(1, 9 + 1))
_COMPRESS_LEVEL_BEST = max(GZIP_COMPRESS_OPTIONS)

# FLG bits
FNAME = 0x8


def open(filename, mode="wb", compresslevel=_COMPRESS_LEVEL_BEST):
    if mode != "wb":
        raise NotImplementedError
    return PigzFile(filename, compresslevel=compresslevel)


class PigzFile:
    def __init__(
        self,
        filename,
        compresslevel=9,
        blocksize=DEFAULT_BLOCK_SIZE_KB,
        workers=CPU_COUNT,
    ):
        """
        Take in a file or directory and gzip using multiple system cores.
        """
        self.filename = filename
        self.compression_level = compresslevel
        self.blocksize = blocksize * 1000

        self.mtime = self._determine_mtime()

        self.output_file = None
        self.output_filename = None

        # This is how we know if we're done reading, compressing, & writing the file
        self._last_chunk = -1
        self._last_chunk_lock = Lock()
        # This is calculated as data is written out
        self.checksum = 0
        # This is calculated as data is read in
        self.input_size = 0

        self.chunk_queue = PriorityQueue()

        # Setup the system threads for compression
        self.pool = Pool(processes=workers)
        # Setup read thread
        self.read_thread = Thread(target=self.read_file)
        # Setup write thread
        self.write_thread = Thread(target=self._write_file)

        # Used to signal the operation is complete for use in the context manager protocol
        self.done_lock = Lock()

        self.input_buffer = io.BytesIO()
        self._first_write = True

    def __enter__(self):
        # self.done_lock.acquire(blocking=True)
        # self._start_all_threads()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        # Wait until compression and write operations are complete
        # while self.done_lock.locked():
        #     pass
        self._clean_up()

    def write(self, data):
        print(f'Took in some data to PigzPython!')
        if isinstance(data, bytes):
            length = len(data)
        else:
            # TODO: This was copied from gzip, is this a bad idea?
            # accept any data that supports the buffer protocol
            data = memoryview(data)
            length = data.nbytes

        if length > 0:
            self.input_buffer.write(data)
            print(f'Just wrote out data to our input buffer!')
            if self._first_write:
                self._first_write = False
                print(f'Starting our threads....')
                self._start_all_threads()



        return length

    def _determine_mtime(self):
        """
        Determine MTIME to write out in Unix format (seconds since Unix epoch).
        From http://www.zlib.org/rfc-gzip.html#header-trailer:
        If the compressed data did not come from a file, MTIME is set to the time at which compression started.
        MTIME = 0 means no time stamp is available.
        """
        try:
            # TODO: This is going to need rethought
            return int(os.stat(self.filename).st_mtime)
        except Exception:
            return int(time.time())

    def _start_all_threads(self):
        """
        Read in the file(s) in chunks.
        Process those chunks.
        Write the resulting file out.
        """
        self._setup_output_file()
        # Start the write thread first so it's ready to accept data
        self.write_thread.start()
        # Start the read thread
        self.read_thread.start()

    def _setup_output_file(self):
        """
        Determine output filename.
        Setup the output file object.
        """
        base = os.path.basename(self.filename)
        self.output_filename = base + ".gz"
        self.output_file = builtins.open(self.output_filename, "wb")

        self._write_output_header()

    def _write_output_header(self):
        """
        Write gzip header to file
        See RFC documentation: http://www.zlib.org/rfc-gzip.html#header-trailer
        """
        # Write ID (IDentification) ID 1, then ID 2. These denote the file as being gzip format.
        self.output_file.write((0x1F).to_bytes(1, sys.byteorder))
        self.output_file.write((0x8B).to_bytes(1, sys.byteorder))
        # Write the CM (compression method)
        self.output_file.write((8).to_bytes(1, sys.byteorder))

        fname = self._determine_fname()
        flags = 0
        if fname:
            flags = FNAME

        # Write FLG (FLaGs)
        self.output_file.write((flags).to_bytes(1, sys.byteorder))

        # Write MTIME (Modification time)
        self.output_file.write((self.mtime).to_bytes(4, sys.byteorder))
        # Write XFL (eXtra FLags)
        extra_flags = self._determine_extra_flags(self.compression_level)
        self.output_file.write((extra_flags).to_bytes(1, sys.byteorder))
        # Write OS
        os_number = self._determine_operating_system()
        self.output_file.write((os_number).to_bytes(1, sys.byteorder))

        # Write the FNAME
        self.output_file.write(fname)

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
        if sys.platform.startswith(("freebsd", "linux", "aix", "darwin")):
            return 3
        elif sys.platform.startswith(("win32")):
            return 0

        return 255

    def _determine_fname(self):
        """
        Determine the FNAME (filename) of the source file to the output
        """
        try:
            # RFC 1952 requires the FNAME field to be Latin-1. Do not
            # include filenames that cannot be represented that way.
            fname = os.path.basename(self.filename)
            if not isinstance(fname, bytes):
                fname = fname.encode("latin-1")
            if fname.endswith(b".gz"):
                fname = fname[:-3]
            # Terminate with zero byte
            fname += b"\0"
        except UnicodeEncodeError:
            fname = b""

        return fname

    def read_file(self):
        """
        Read {filename} in {blocksize} chunks.
        This method is run on the read thread.
        """
        chunk_num = 0
        # with builtins.open(self.input_buffer, "rb") as input_buffer:
        while True:
            chunk = self.input_buffer.read(self.blocksize)
            # Break out of the loop if we didn't read anything
            if not chunk:
                # Since we previously advanced chunk_num counter before we knew we reached EOF, decrement 1
                with self._last_chunk_lock:
                    self._last_chunk = chunk_num - 1
                print(f'Read out the last bit of input data!!!')
                print(f'Setting last chunk to: {self._last_chunk}')
                break

            self.input_size += len(chunk)
            # Apply this chunk to the pool
            self.pool.apply_async(self.process_chunk, (chunk_num, chunk))
            chunk_num += 1

    def process_chunk(self, chunk_num: int, chunk: bytes):
        """
        Overall method to handle the chunk and pass it back to the write thread.
        This method is run on the pool.
        """
        with self._last_chunk_lock:
            last_chunk = True if chunk_num == self._last_chunk else False
        compressed_chunk = self._compress_chunk(chunk, last_chunk)
        self.chunk_queue.put((chunk_num, chunk, compressed_chunk))

    def _compress_chunk(self, chunk: bytes, is_last_chunk: bool):
        """
        Compress the chunk.
        """
        compressor = zlib.compressobj(
            level=self.compression_level,
            method=zlib.DEFLATED,
            wbits=-zlib.MAX_WBITS,
            memLevel=zlib.DEF_MEM_LEVEL,
            strategy=zlib.Z_DEFAULT_STRATEGY,
        )
        compressed_data = compressor.compress(chunk)
        if is_last_chunk:
            compressed_data += compressor.flush(zlib.Z_FINISH)
        else:
            compressed_data += compressor.flush(zlib.Z_SYNC_FLUSH)

        return compressed_data

    def _write_file(self):
        """
        Write compressed data to disk.
        Read chunks off of the priority queue.
        Priority is the chunk number, so we can keep track of which chunk to get next.
        This is run from the write thread.
        """
        next_chunk_num = 0
        while True:
            if not self.chunk_queue.empty():
                chunk_num, chunk, compressed_chunk = self.chunk_queue.get()

                if chunk_num != next_chunk_num:
                    # If this isn't the next chunk we're looking for, place it back on the queue and sleep
                    self.chunk_queue.put((chunk_num, chunk, compressed_chunk))
                    time.sleep(0.5)
                else:
                    # Calculate running checksum
                    self._calculate_chunk_check(chunk)
                    # Write chunk to file, advance next chunk we're looking for
                    self.output_file.write(compressed_chunk)
                    # If this was the last chunk, we can break the loop and close the file
                    if chunk_num == self._last_chunk:
                        break
                    next_chunk_num += 1
            else:
                # If the queue is empty, we're likely waiting for data.
                time.sleep(0.5)
        # Loop breaks out if we've received the final chunk
        self._clean_up()

    def _calculate_chunk_check(self, chunk: bytes):
        """
        Calculate the check value for the chunk.
        """
        self.checksum = zlib.crc32(chunk, self.checksum)

    def _clean_up(self):
        """
        Close the output file.
        Clean up the processing pool.
        Release the lock to indicate that the compression operation is complete.
        """
        self._write_file_trailer()

        # Flush internal buffers
        self.output_file.flush()
        self.output_file.close()

        self._close_workers()

        # self.done_lock.release()

    def _write_file_trailer(self):
        """
        Write the trailer for the compressed data.
        """
        # Write CRC32
        self.output_file.write((self.checksum).to_bytes(4, sys.byteorder))
        # Write ISIZE (Input SIZE) - This contains the size of the original (uncompressed) input data modulo 2^32.
        self.output_file.write(
            (self.input_size & 0xFFFFFFFF).to_bytes(4, sys.byteorder)
        )

    def _close_workers(self):
        """
        Stop threads and close pool.
        """
        self.pool.terminate()
        self.pool.join()


def compress_file(source_file):
    # This really should just do the context manager protocol work for us given a valid file path
    with builtins.open(source_file, "rb") as f_in:
        with PigzFile(source_file) as f_out:
            print(f'PigzFile type is: {type(f_out)}')
            shutil.copyfileobj(f_in, f_out)

    # Old reliable way to do this before API change
    # foo = PigzFile(source_file)
    # foo._start_all_threads()
