# pigz-python
The goal of this project is to create a pure Python implementation of the pigz project for parallelizing gzipping.


# Usage examples

pigz-python can be utilized by creating a `PigzFile` object and calling the `process_compression_target()` method.

```python
from pigz_python import PigzFile

pigz_file = PigzFile('foo.txt')
pigz_file.process_compression_target()
```

Alternatively, the pigz_python module also provides a convenient helper method to do all of this work for you.

```python
import pigz_python

pigz_python.compress_file('foo.txt')
```
