# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

pigz-python is a pure Python implementation of the pigz (parallel gzip) utility that accelerates file compression by utilizing multiple CPU cores. It uses a producer-consumer threading model with a multiprocessing pool for parallel compression.

## Common Commands

```bash
# Install in development mode
pip install -e .

# Run tests
pytest
pytest tests/test_pigz_python.py -v

# Run specific test
pytest tests/test_pigz_python.py::TestPigzPython::test_determine_operating_system_windows

# Run tests across Python versions (3.6, 3.7, 3.8)
tox
tox -e py38
```

## Architecture

### Threading Model

The `PigzFile` class implements a three-stage pipeline:

1. **Read Thread** (`_read_file`) - Reads source file in configurable chunks (default 128KB), submits chunks to worker pool
2. **Compression Pool** - `multiprocessing.dummy.Pool` with workers (default: CPU count) that compress chunks using zlib
3. **Write Thread** (`_write_file`) - Consumes from a `PriorityQueue` to write compressed chunks in order

Synchronization uses a thread-safe PriorityQueue and Lock for the `_last_chunk` flag.

### Key Implementation Details

- Uses `zlib.Z_SYNC_FLUSH` for non-final chunks to maintain stream continuity
- Uses `zlib.Z_FINISH` for the final chunk
- Output file is created in the same directory as input (e.g., `foo.txt` → `foo.txt.gz`)
- Implements RFC 1952 gzip format with proper header/trailer

### Entry Points

```python
# Option 1: Class-based
from pigz_python import PigzFile
pigz_file = PigzFile('foo.txt')
pigz_file.process_compression_target()

# Option 2: Helper function
import pigz_python
pigz_python.compress_file('foo.txt')
```

## Code Style

- Max line length: 88 (Black compatible)
- Linting: flake8, pycodestyle, pylint
- Standard library only (no external runtime dependencies)
