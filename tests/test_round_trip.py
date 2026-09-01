"""
Round-trip tests for Pigz Python.

These compress real files and decompress them with the standard library, so
they validate output rather than internal call patterns. Every payload is
generated into pytest's tmp_path; nothing here depends on files outside the
repository.
"""

import pytest

from tests.helpers import (
    DATA_SHAPES,
    KB,
    MB,
    assert_identical,
    assert_round_trip,
    compressible_text,
    incompressible_bytes,
    structured_binary,
)

# Small enough to keep the suite quick, large enough to span many blocks when
# the tests below shrink the blocksize.
SAMPLE_SIZE = 256 * KB


@pytest.mark.parametrize("shape", sorted(DATA_SHAPES))
def test_round_trip_data_shapes(tmp_path, shape):
    """Compressible, incompressible, and structured payloads all survive."""
    data = DATA_SHAPES[shape](MB)
    assert_round_trip(tmp_path, f"{shape}.bin", data)


@pytest.mark.parametrize("compresslevel", [1, 6, 9])
def test_round_trip_compression_levels(tmp_path, compresslevel):
    """Every compression level produces a faithful archive."""
    data = structured_binary(SAMPLE_SIZE)
    assert_round_trip(tmp_path, "levels.bin", data, compresslevel=compresslevel)


@pytest.mark.parametrize(
    "blocksize,workers",
    [
        (1, 1),
        (1, 8),
        (4, 16),
        (128, 1),
        (128, 8),
    ],
)
def test_round_trip_blocksize_and_workers(tmp_path, blocksize, workers):
    """
    Chunking and worker count do not affect the output.

    A 1 KB blocksize splits the payload into hundreds of chunks, which forces
    the write thread to reassemble many out-of-order results.
    """
    data = incompressible_bytes(SAMPLE_SIZE)
    assert_round_trip(
        tmp_path, "chunked.bin", data, blocksize=blocksize, workers=workers
    )


def test_round_trip_via_pigzfile_class(tmp_path):
    """The PigzFile class entry point round-trips as well as compress_file()."""
    data = compressible_text(SAMPLE_SIZE)
    assert_round_trip(tmp_path, "class_entry_point.txt", data, use_class=True)


def test_round_trip_multi_megabyte_payload(tmp_path):
    """A payload spanning many default-sized blocks survives intact."""
    data = incompressible_bytes(4 * MB)
    assert_round_trip(tmp_path, "large.bin", data)


def test_archive_is_written_next_to_source(tmp_path):
    """Output lands beside the input as <name>.gz, per the documented behavior."""
    source_dir = tmp_path / "nested"
    source_dir.mkdir()
    archive = assert_round_trip(source_dir, "report.txt", compressible_text(8 * KB))

    assert archive.parent == source_dir
    assert archive.name == "report.txt.gz"


def test_compressible_payload_actually_shrinks(tmp_path):
    """
    Guard against a regression that emits valid but uncompressed output.

    A round trip alone would still pass if the deflate stream stored the data
    verbatim, so assert repeating text gets substantially smaller.
    """
    data = compressible_text(SAMPLE_SIZE)
    archive = assert_round_trip(tmp_path, "shrinks.txt", data)

    assert archive.stat().st_size < len(data) // 10


def test_higher_compression_level_produces_smaller_archive(tmp_path):
    """The compresslevel argument reaches zlib rather than being ignored."""
    data = compressible_text(SAMPLE_SIZE)

    fastest = assert_round_trip(tmp_path, "fastest.txt", data, compresslevel=1)
    best = assert_round_trip(tmp_path, "best.txt", data, compresslevel=9)

    assert best.stat().st_size < fastest.stat().st_size


def test_repeated_compression_is_deterministic(tmp_path):
    """Two runs over identical input produce identical compressed streams."""
    data = structured_binary(SAMPLE_SIZE)

    first_dir = tmp_path / "first"
    second_dir = tmp_path / "second"
    first_dir.mkdir()
    second_dir.mkdir()

    first = assert_round_trip(first_dir, "stable.bin", data, workers=1)
    second = assert_round_trip(second_dir, "stable.bin", data, workers=8)

    # Skip the 4-byte MTIME at offset 4, which differs with the source file.
    first_bytes = first.read_bytes()
    second_bytes = second.read_bytes()
    assert_identical(
        second_bytes[:4] + second_bytes[8:],
        first_bytes[:4] + first_bytes[8:],
        "compressed stream across runs",
    )
