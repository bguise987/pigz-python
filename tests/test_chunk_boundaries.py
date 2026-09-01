"""
Chunk boundary tests for Pigz Python.

The read thread peeks one block ahead to decide which chunk is the last one, so
the input sizes that matter are those on and immediately around a block
boundary, plus the degenerate case of no blocks at all. A miscount here does
not corrupt bytes so much as strand the write thread, which is why every test
in this module runs under a timeout.
"""

import struct

import pytest

from pigz_python import pigz_python
from tests.helpers import (
    assert_round_trip,
    block_size_in_bytes,
    compressible_text,
    incompressible_bytes,
)

# A 1 KB block keeps these payloads tiny while still spanning several chunks.
BLOCKSIZE_KB = 1
BLOCK = block_size_in_bytes(BLOCKSIZE_KB)

# Nothing here is more than a few KB, so anything this slow is wedged.
TIMEOUT_SECONDS = 20


@pytest.mark.parametrize(
    "size",
    [
        0,
        1,
        BLOCK - 1,
        BLOCK,
        BLOCK + 1,
        2 * BLOCK - 1,
        2 * BLOCK,
        2 * BLOCK + 1,
        5 * BLOCK,
    ],
)
def test_round_trip_at_block_boundaries(tmp_path, size):
    """Every size on or beside an exact block multiple survives a round trip."""
    assert_round_trip(
        tmp_path,
        f"boundary_{size}.txt",
        compressible_text(size),
        blocksize=BLOCKSIZE_KB,
        timeout=TIMEOUT_SECONDS,
    )


@pytest.mark.parametrize("blocks", [1, 2, 3])
def test_exact_multiple_of_default_blocksize(tmp_path, blocks):
    """
    The boundary also holds at the shipped 128 KB default.

    Incompressible data here so the payload cannot collapse to a single small
    chunk and quietly skip the multi-block path.
    """
    size = blocks * block_size_in_bytes(pigz_python.DEFAULT_BLOCK_SIZE_KB)
    assert_round_trip(
        tmp_path,
        f"default_blocks_{blocks}.bin",
        incompressible_bytes(size),
        timeout=TIMEOUT_SECONDS,
    )


def test_empty_file_terminates_and_round_trips(tmp_path):
    """
    A zero-byte source must not strand the write thread.

    The read loop produces no chunks for an empty file, so nothing ever marks a
    last chunk; the writer then polls an empty queue forever and
    process_compression_target() never returns.
    """
    assert_round_trip(tmp_path, "empty.txt", b"", timeout=TIMEOUT_SECONDS)


def test_empty_file_trailer_is_zeroed(tmp_path):
    """An empty source records a zero checksum and zero input size."""
    archive = assert_round_trip(tmp_path, "empty.txt", b"", timeout=TIMEOUT_SECONDS)

    checksum, input_size = struct.unpack("<II", archive.read_bytes()[-8:])

    assert checksum == 0
    assert input_size == 0


def test_single_byte_file(tmp_path):
    """The smallest non-empty input still takes the single-chunk path."""
    assert_round_trip(tmp_path, "one_byte.bin", b"\x00", timeout=TIMEOUT_SECONDS)
