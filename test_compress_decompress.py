import shutil

from pathlib import Path
import hashlib
import gzip

from pigz_python import pigz_python


# https://stackoverflow.com/a/59056837

# PDF File
pdf_file_name = 'TEACH_YOURSELF_SQL_10MINS.pdf'
pdf_file_name_compressed = f'{pdf_file_name}.gz'
pdf_file_blake2b_hash = '0df23583092f8f5718d318615a25767ae35e9d9bf3dd01f8d9c220f7fc68c9636167cef02e8f959bbbb413c38e9b20fdb26706e1eeefafd268c1a4bc91aac52d'


test_file_name = pdf_file_name
test_file_compressed = pdf_file_name_compressed
test_file_blake2b_hash = pdf_file_blake2b_hash


while True:

    print(f'Now compressing the test file...')
    # Compress the test file, saving the original
    test_file_path = Path('C:/','Users','ben.guise','Desktop', test_file_name)
    pigz_python.compress_file(test_file_path)

    print(f'Now moving the resulting compressed file into test dir...')
    # Move file into the foo dir
    shutil.move(Path('C:/','Users','ben.guise','Desktop',test_file_compressed), Path('C:/','Users','ben.guise','Desktop','foo',test_file_compressed))
    print(f'Decompressing the resulting gzip...')
    # Decompress the test file
    with gzip.open(Path('C:/','Users','ben.guise','Desktop','foo',test_file_compressed), 'rb') as f_in:
        with open(Path('C:/','Users','ben.guise','Desktop','foo',test_file_name), 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

    print(f'Checking the blake2b digest for resulting decompressed file so we can compare to source...')
    # Check the blake2b of the resulting decompression
    with open(Path('C:/','Users','ben.guise','Desktop','foo',test_file_name), "rb") as f:
        file_hash = hashlib.blake2b()
        while chunk := f.read(8192):
            file_hash.update(chunk)

    print(f'blake2b hex digest for the decompressed file {file_hash.hexdigest()}')  # to get a printable str instead of bytes

    if file_hash.hexdigest() == test_file_blake2b_hash:
        # If the blake2b matches, delete the compressed file, repeat
        print(f'The hex digest matches for source and result files. Now deleting result file and compressed file to repeat the experiment...')
        Path('C:/','Users','ben.guise','Desktop','foo',test_file_name).unlink()
        Path('C:/','Users','ben.guise','Desktop','foo',test_file_compressed).unlink()
        print(f'\n\n\n\n\n')
    else:
        # We hit the bug, break out of the while loop
        print(f'Oh no! The blake2b hash for our decompressed file DOES NOT match that of the source file!')
        break
