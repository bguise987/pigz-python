"""
Bounded concurrency stress tests for Pigz Python.

These replace the open-ended "run it in a loop until something breaks" script
that used to live in the repository root. Everything here is marked slow and is
deselected from the default suite; CI runs it in a dedicated job with -m slow.

The two tests below target different failure modes, and the settings that
provoke one are close to useless against the other:

* Last-chunk detection (the race fixed in 2026). The window is between the read
  thread submitting the final chunk and it recording _last_chunk. That window
  only stays open while the pool is idle enough to start the final chunk
  immediately, which means SMALL inputs of a few chunks. Measured against the
  pre-fix implementation, inputs of one to four chunks corrupted up to 16 runs
  in 60, while a 4 MB input split into ~4000 chunks never failed once: the
  final chunk sits so far back in the pool's queue that the window has closed
  long before a worker reaches it. Do not "strengthen" this test by making the
  payload bigger or the blocks smaller; that is what makes it stop working.

* Chunk reassembly. The write thread reorders chunks through a PriorityQueue,
  and that path needs the opposite input: many small blocks so results arrive
  out of order.
"""

from pathlib import Path

import pytest

from tests.helpers import (
    MB,
    assert_round_trip,
    block_size_in_bytes,
    incompressible_bytes,
)

# Sizes chosen to span one to four chunks at the default 128 KB blocksize,
# where the last-chunk race is actually reachable.
DEFAULT_BLOCK = block_size_in_bytes(128)
SMALL_INPUT_SIZES = [
    5_000,
    DEFAULT_BLOCK + 72_000,
    2 * DEFAULT_BLOCK + 44_000,
    4 * DEFAULT_BLOCK - 12_000,
]

# Per-run failure rates against the pre-fix code ranged from roughly 5% to 27%,
# so this many repeats makes a surviving regression very unlikely to slip past.
RACE_ITERATIONS = 30

# Reassembly needs far fewer repeats; ordering bugs are not timing-marginal in
# the same way, and each run here already spans thousands of chunks.
REASSEMBLY_ITERATIONS = 10

# More workers than any runner has cores, so the pool is never the bottleneck.
WORKERS = 20

# Generous next to a healthy ~0.5s run, but still fails a wedged pipeline in
# bounded time rather than hanging CI.
STRESS_TIMEOUT_SECONDS = 120


def _round_trip_and_clean(directory, filename, payload, blocksize, workers):
    """Round-trip one payload, then delete both files so disk use stays flat."""
    archive = assert_round_trip(
        directory,
        filename,
        payload,
        blocksize=blocksize,
        workers=workers,
        timeout=STRESS_TIMEOUT_SECONDS,
    )
    archive.unlink()
    Path(directory, filename).unlink()


@pytest.mark.slow
@pytest.mark.parametrize("size", SMALL_INPUT_SIZES)
def test_final_chunk_is_always_terminated(tmp_path, size):
    """
    Repeatedly compress a small input with an idle pool of many workers.

    If the final chunk is ever compressed with Z_SYNC_FLUSH instead of
    Z_FINISH, the deflate stream is left unterminated and the archive fails to
    decompress. The round-trip assertion catches that; the timeout inside
    helpers.compress() catches the case where no chunk is marked last at all
    and the write thread polls forever.
    """
    payload = incompressible_bytes(size)
    for iteration in range(RACE_ITERATIONS):
        _round_trip_and_clean(
            tmp_path,
            f"race_{size}_{iteration}.bin",
            payload,
            blocksize=128,
            workers=WORKERS,
        )


@pytest.mark.slow
@pytest.mark.parametrize("blocksize", [1, 4])
def test_chunks_reassemble_in_order(tmp_path, blocksize):
    """
    Repeatedly compress a multi-MB payload split into thousands of blocks.

    Small blocks with many workers guarantee chunks finish out of order, so
    this exercises the write thread's PriorityQueue reassembly rather than the
    last-chunk flag.
    """
    payload = incompressible_bytes(4 * MB)
    for iteration in range(REASSEMBLY_ITERATIONS):
        _round_trip_and_clean(
            tmp_path,
            f"reassembly_{blocksize}kb_{iteration}.bin",
            payload,
            blocksize=blocksize,
            workers=WORKERS,
        )
