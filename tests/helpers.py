"""
Shared helpers for the behavioral (non-mock) pigz-python tests.

Test payloads are generated on the fly rather than committed, so the suite has
no dependence on files outside the repository. Generators are deterministic so
a failure can be reproduced exactly.
"""

import gzip
import random
import struct
from pathlib import Path

from pigz_python import pigz_python

KB = 1024
MB = 1024 * KB

# PigzFile takes its blocksize in "KB" but multiplies by 1000, not 1024, so a
# block is not the same size as KB above. Tests that need to land on an exact
# block boundary must use block_size_in_bytes() rather than assuming 1024.
BLOCK_SIZE_UNIT = 1000


def block_size_in_bytes(blocksize_kb):
    """Return the true byte size of a PigzFile blocksize given in KB."""
    return blocksize_kb * BLOCK_SIZE_UNIT


def compressible_text(size):
    """Return `size` bytes of highly compressible repeating text."""
    block = (
        b"Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do "
        b"eiusmod tempor incididunt ut labore et dolore magna aliqua.\n"
    )
    repeats = size // len(block) + 1
    return (block * repeats)[:size]


def incompressible_bytes(size, seed=20260831):
    """Return `size` bytes of deterministic pseudo-random, incompressible data."""
    return random.Random(seed).randbytes(size)


def structured_binary(size, seed=8675309):
    """
    Return `size` bytes of struct-packed fixed-width records.

    This sits between the two extremes above: regular framing that deflate can
    exploit, wrapped around values it cannot.
    """
    rng = random.Random(seed)
    record = struct.Struct("<IHHd")
    chunks = []
    written = 0
    index = 0
    while written < size:
        packed = record.pack(index, index % 7, rng.randrange(1000), index * 1.5)
        chunks.append(packed)
        written += len(packed)
        index += 1
    return b"".join(chunks)[:size]


DATA_SHAPES = {
    "compressible_text": compressible_text,
    "incompressible_bytes": incompressible_bytes,
    "structured_binary": structured_binary,
}


def write_source_file(directory, filename, data):
    """Write `data` to `filename` inside `directory` and return the path."""
    source = Path(directory, filename)
    source.write_bytes(data)
    return source


def compress(
    source,
    compresslevel=pigz_python._COMPRESS_LEVEL_BEST,  # pylint: disable=protected-access
    blocksize=pigz_python.DEFAULT_BLOCK_SIZE_KB,
    workers=pigz_python.CPU_COUNT,
    use_class=False,
):
    """
    Compress `source` with pigz-python and return the path to the archive.

    Set use_class to drive the PigzFile class directly instead of going through
    the compress_file() helper, so both public entry points get exercised.
    """
    source = Path(source)
    if use_class:
        pigz_file = pigz_python.PigzFile(source, compresslevel, blocksize, workers)
        pigz_file.process_compression_target()
    else:
        pigz_python.compress_file(source, compresslevel, blocksize, workers)
    return Path(source.parent, source.name + ".gz")


def decompress(archive):
    """Decompress `archive` with the standard library and return its bytes."""
    with gzip.open(archive, "rb") as archive_file:
        return archive_file.read()


def assert_identical(actual, expected, context):
    """
    Assert two byte strings match, reporting where they diverge.

    Payloads here run to megabytes, so a plain `assert actual == expected`
    would dump an unreadable diff. This reports the length or the offset of the
    first differing byte instead.
    """
    if actual == expected:
        return
    if len(actual) != len(expected):
        raise AssertionError(
            f"{context}: length mismatch, got {len(actual)} bytes, "
            f"expected {len(expected)} bytes"
        )
    offset = next(
        index for index, (got, want) in enumerate(zip(actual, expected)) if got != want
    )
    raise AssertionError(
        f"{context}: contents diverge at byte {offset} of {len(expected)}, "
        f"got {actual[offset:offset + 8]!r}, expected {expected[offset:offset + 8]!r}"
    )


def assert_round_trip(directory, filename, data, **compress_kwargs):
    """
    Write `data` to `filename`, compress it, decompress the result with the
    standard library, and assert the bytes survived unchanged.

    Returns the archive path so callers can make further assertions about it.
    """
    source = write_source_file(directory, filename, data)
    archive = compress(source, **compress_kwargs)

    assert archive.exists(), f"no archive was produced at {archive}"

    assert_identical(decompress(archive), data, f"round trip of {filename}")
    assert_identical(
        source.read_bytes(), data, f"source file {filename} after compress"
    )
    return archive
