"""
Unit tests for Pigz Python
"""
import sys
import unittest
import zlib
from pathlib import Path
from unittest.mock import MagicMock, Mock, call, mock_open, patch

import pigz_python.pigz_python as pigz_python

LOREM_IPSUM_FILE = "lorem_ipsum.txt"


# pylint: disable=protected-access, too-many-public-methods
class TestPigzPython(unittest.TestCase):
    """ Unit tests for PigzPython class """

    def setUp(self):
        """
        Setup PigzFile instance for testing, using a mock file.
        """
        test_file = Path("tests", LOREM_IPSUM_FILE)
        self.pigz_file = pigz_python.PigzFile(test_file)

    def test_compress_directory_raise_error(self):
        """
        Test that PigzFile raises NotImplementedError when given a directory
        """
        with self.assertRaises(NotImplementedError):
            pigz_python.PigzFile(Path("tests"))

    def test_compress_nonexistant_file_raise_error(self):
        """
        Test that PigzFile raises FileNotFoundError when given
        a file that doesn't exist
        """
        with self.assertRaises(FileNotFoundError):
            pigz_python.PigzFile(Path("tests", "fake_file.txt"))

    def test_determine_operating_system_windows(self):
        """
        Test finding operating system on Windows
        """
        mock_sys_platform = "win32"
        expected_operating_system = 0
        with patch(
            "sys.platform", new_callable=MagicMock(return_value=mock_sys_platform)
        ):
            operating_system = pigz_python.PigzFile._determine_operating_system()
        self.assertEqual(operating_system, expected_operating_system)

    def test_determine_operating_system_freebsd(self):
        """
        Test finding operating system on FreeBSD
        """
        mock_sys_platform = "freebsd"
        expected_operating_system = 3
        with patch(
            "sys.platform", new_callable=MagicMock(return_value=mock_sys_platform)
        ):
            operating_system = pigz_python.PigzFile._determine_operating_system()
        self.assertEqual(operating_system, expected_operating_system)

    def test_determine_operating_system_linux(self):
        """
        Test finding operating system on Linux
        """
        mock_sys_platform = "linux"
        expected_operating_system = 3
        with patch(
            "sys.platform", new_callable=MagicMock(return_value=mock_sys_platform)
        ):
            operating_system = pigz_python.PigzFile._determine_operating_system()
        self.assertEqual(operating_system, expected_operating_system)

    def test_determine_operating_system_aix(self):
        """
        Test finding operating system on AIX
        """
        mock_sys_platform = "aix"
        expected_operating_system = 3
        with patch(
            "sys.platform", new_callable=MagicMock(return_value=mock_sys_platform)
        ):
            operating_system = pigz_python.PigzFile._determine_operating_system()
        self.assertEqual(operating_system, expected_operating_system)

    def test_determine_operating_system_darwin(self):
        """
        Test finding operating system on Darwin
        """
        mock_sys_platform = "darwin"
        expected_operating_system = 3
        with patch(
            "sys.platform", new_callable=MagicMock(return_value=mock_sys_platform)
        ):
            operating_system = pigz_python.PigzFile._determine_operating_system()
        self.assertEqual(operating_system, expected_operating_system)

    def test_determine_operating_system_non_standard(self):
        """
        Test finding operating system on a non-standard OS
        """
        mock_sys_platform = "vms"
        expected_operating_system = 255
        with patch(
            "sys.platform", new_callable=MagicMock(return_value=mock_sys_platform)
        ):
            operating_system = pigz_python.PigzFile._determine_operating_system()
        self.assertEqual(operating_system, expected_operating_system)

    def test_determine_extra_flags_max_compression(self):
        """
        Ensure appropriate XFL or eXtra FLags value is returned for
        max compression values
        """
        compression_levels = [9, 10, 11, 12]
        expected_xfl = 2
        for value in compression_levels:
            xfl = pigz_python.PigzFile._determine_extra_flags(value)
            self.assertEqual(xfl, expected_xfl)

    def test_determine_extra_flags_min_compression(self):
        """
        Ensure appropriate XFL or eXtra FLags value is returned for
        min compression value
        """
        compression_level = 1
        expected_xfl = 4
        xfl = pigz_python.PigzFile._determine_extra_flags(compression_level)
        self.assertEqual(xfl, expected_xfl)

    def test_determine_extra_flags_default_compression(self):
        """
        Ensure appropriate XFL or eXtra FLags value is returned for values
        not max or min
        """
        compression_levels = [0, 2, 3, 4, 5, 6, 7, 8]
        expected_xfl = 0
        for value in compression_levels:
            xfl = pigz_python.PigzFile._determine_extra_flags(value)
            self.assertEqual(xfl, expected_xfl)

    def test_set_output_filename(self):
        """
        Ensure output filenames are appropriate
        """
        expected_output_filename = f"{LOREM_IPSUM_FILE}.gz"
        self.pigz_file._set_output_filename()
        self.assertEqual(self.pigz_file.output_filename, expected_output_filename)

    def test_determine_fname_basic_case(self):
        """
        Test that fname is returned properly for a latin-1 encoded filename
        """
        filename = "Golden_Ticket.txt"
        fname = pigz_python.PigzFile._determine_fname(filename)

        filename = filename.encode("latin-1")
        # Correct fnames are terminated with zero byte
        expected_filename = filename + b"\0"
        self.assertEqual(fname, expected_filename)

    def test_determine_fname_gz_extension(self):
        """
        Test that fname is returned properly for a file with a .gz extension
        """
        filename = "Golden_Ticket.mp3.gz"
        fname = pigz_python.PigzFile._determine_fname(filename)

        # Trim the .gz from our sample filename
        filename = filename[:-3].encode("latin-1")
        # Correct fnames are terminated with zero byte
        expected_filename = filename + b"\0"
        self.assertEqual(fname, expected_filename)

    def test_determine_fname_non_latin_1(self):
        """
        Test that fname is returned properly for a file that
        is not able to be encoded in latin-1
        """
        filename = "В Питере — пить.mp3"
        fname = pigz_python.PigzFile._determine_fname(filename)
        # If FNAME cannot be Latin-1 encoded, return empty FNAME
        expected_filename = b""
        self.assertEqual(fname, expected_filename)

    def test_compress_chunk_last_chunk(self):
        """
        Test compressing data when it is the last chunk
        """
        input_data = b"This is a test string"
        # This output data was generated with compression level 9
        # As the test is written, if the PigzFile default is changed,
        # this test data may also need to be updated.
        expected_output = (
            b"\x0b\xc9\xc8,V\x00\xa2D\x85\x92\xd4\xe2\x12\x85\xe2\x92\xa2\xcc\xbct\x00"
        )
        compressed_data = self.pigz_file._compress_chunk(input_data, is_last_chunk=True)
        self.assertEqual(compressed_data, expected_output)

    def test_compress_chunk_not_last_chunk(self):
        """
        Test compressing data when it is NOT the last chunk
        """
        input_data = b"This is a test string"
        # This output data was generated with compression level 9
        # As the test is written, if the PigzFile default is changed,
        # this test data may also need to be updated.
        expected_output = b"\n\xc9\xc8,V\x00\xa2D\x85\x92\xd4\xe2\x12\x85\xe2\x92\xa2\xcc\xbct\x00\x00\x00\x00\xff\xff"  # noqa; pylint: disable=line-too-long
        compressed_data = self.pigz_file._compress_chunk(
            input_data, is_last_chunk=False
        )
        self.assertEqual(compressed_data, expected_output)

    def test_close_workers(self):
        """
        Test that compression worker pool closed.
        """
        self.pigz_file.pool = MagicMock()
        self.pigz_file._close_workers()
        self.pigz_file.pool.close.assert_called_once()
        self.pigz_file.pool.join.assert_called_once()

    def test_process_compression_target(self):
        """
        Test that appropriate methods are called to compress the file
        """
        # Setup mocks
        self.pigz_file._setup_output_file = MagicMock()
        self.pigz_file.write_thread = MagicMock()
        self.pigz_file.read_thread = MagicMock()

        # Call the target method
        self.pigz_file.process_compression_target()

        # Assert appropriate methods called
        self.pigz_file._setup_output_file.assert_called_once()
        self.pigz_file.write_thread.start.assert_called_once()
        self.pigz_file.read_thread.start.assert_called_once()
        self.pigz_file.write_thread.join.assert_called_once()

    def test_setup_output_file(self):
        """
        Test that we properly setup the output gzip file
        """
        filename = "fun_filename.txt.gz"
        self.pigz_file._set_output_filename = MagicMock()
        self.pigz_file._write_output_header = MagicMock()
        self.pigz_file.output_filename = filename

        with patch("builtins.open", mock_open(read_data="data")) as mock_file:
            self.pigz_file._setup_output_file()
            # Assert output file opened appropriately
            mock_file.assert_called_with(filename, "wb")

            # Assert methods called
            self.pigz_file._set_output_filename.assert_called_once()
            self.pigz_file._write_output_header.assert_called_once()

    def test_calculate_chunk_check_from_zero(self):
        """
        Test that the crc32 check is properly handled from 0
        """
        input_data = b"really fun data"
        checksum_init = 0
        expected_checksum = zlib.crc32(input_data, checksum_init)

        self.pigz_file.calculate_chunk_check(input_data)

        self.assertEqual(self.pigz_file.checksum, expected_checksum)

    def test_calculate_chunk_check_from_nonzero(self):
        """
        Test that the crc32 check is properly handled from nonzero
        """
        input_data1 = b"really fun data"
        input_data2 = b"MORE fun data!"
        running_checksum = 0
        running_checksum = zlib.crc32(input_data1, running_checksum)
        expected_checksum = zlib.crc32(input_data2, running_checksum)

        # Initialize checksum
        self.pigz_file.checksum = 2322970659
        # Calculate for second set of data
        self.pigz_file.calculate_chunk_check(input_data2)

        self.assertEqual(self.pigz_file.checksum, expected_checksum)

    def test_write_header_id(self):
        """
        Test that we properly write the ID1 and ID2 fields of the gzip header
        """
        self.pigz_file.output_file = MagicMock()
        ID1 = (0x1F).to_bytes(1, sys.byteorder)  # pylint: disable=invalid-name
        ID2 = (0x8B).to_bytes(1, sys.byteorder)  # pylint: disable=invalid-name
        with patch("sys.byteorder", "little"):
            self.pigz_file._write_header_id()
            self.pigz_file.output_file.write.assert_has_calls([call(ID1), call(ID2)])

    def test_write_header_cm(self):
        """
        Test that we properly write the CM field of the gzip header
        """
        self.pigz_file.output_file = MagicMock()
        CM = (8).to_bytes(1, sys.byteorder)  # pylint: disable=invalid-name
        with patch("sys.byteorder", "little"):
            self.pigz_file._write_header_cm()
            self.pigz_file.output_file.write.assert_has_calls([call(CM)])

    def test_write_header_flg(self):
        """
        Test that we properly write the FLG field of the gzip header
        """
        self.pigz_file.output_file = MagicMock()
        flags = 0xA
        write_flags = (flags).to_bytes(1, sys.byteorder)
        with patch("sys.byteorder", "little"):
            self.pigz_file._write_header_flg(flags)
            self.pigz_file.output_file.write.assert_has_calls([call(write_flags)])

    def test_write_output_header_with_fname(self):
        """
        Test that the output header is written with the FNAME field
        """
        # Setup mocks
        self.pigz_file.output_file = MagicMock()
        self.pigz_file.compression_target = "foo.txt"
        self.pigz_file._write_header_id = MagicMock()
        self.pigz_file._write_header_cm = MagicMock()
        self.pigz_file._write_header_flg = MagicMock()
        self.pigz_file._write_header_mtime = MagicMock()
        self.pigz_file._write_header_xfl = MagicMock()
        self.pigz_file._write_header_os = MagicMock()
        # Make the call
        self.pigz_file._write_output_header()
        # Assertions
        self.pigz_file._write_header_id.assert_called_once()
        self.pigz_file._write_header_cm.assert_called_once()
        self.pigz_file._write_header_flg.assert_called_with(pigz_python.FNAME)
        self.pigz_file._write_header_mtime.assert_called_once()
        self.pigz_file._write_header_xfl.assert_called_once()
        self.pigz_file._write_header_os.assert_called_once()

    def test_write_output_header_without_fname(self):
        """
        Test that the output header is written without the FNAME field
        """
        # Setup mocks
        self.pigz_file.output_file = MagicMock()
        self.pigz_file.compression_target = "В Питере — пить.mp3"
        self.pigz_file._write_header_id = MagicMock()
        self.pigz_file._write_header_cm = MagicMock()
        self.pigz_file._write_header_flg = MagicMock()
        self.pigz_file._write_header_mtime = MagicMock()
        self.pigz_file._write_header_xfl = MagicMock()
        self.pigz_file._write_header_os = MagicMock()
        # Make the call
        self.pigz_file._write_output_header()
        # Assertions
        self.pigz_file._write_header_id.assert_called_once()
        self.pigz_file._write_header_cm.assert_called_once()

        self.pigz_file._write_header_flg.assert_called_with(0x0)

        self.pigz_file._write_header_mtime.assert_called_once()
        self.pigz_file._write_header_xfl.assert_called_once()
        self.pigz_file._write_header_os.assert_called_once()

    def test_write_header_mtime(self):
        """
        Test writing out the mtime to the header
        """
        test_time = 8675309
        test_time_bytes = (test_time).to_bytes(4, sys.byteorder)
        self.pigz_file._determine_mtime = MagicMock()
        self.pigz_file._determine_mtime.return_value = test_time
        self.pigz_file.output_file = MagicMock()

        self.pigz_file._write_header_mtime()

        self.pigz_file.output_file.write.assert_called_with(test_time_bytes)

    def test_write_header_xfl(self):
        """
        Test writing out xfl to the header
        """
        extra_flags = 2
        extra_flags_bytes = (extra_flags).to_bytes(1, sys.byteorder)
        self.pigz_file._determine_extra_flags = MagicMock()
        self.pigz_file._determine_extra_flags.return_value = extra_flags
        self.pigz_file.output_file = MagicMock()

        self.pigz_file._write_header_xfl()

        self.pigz_file.output_file.write.assert_called_with(extra_flags_bytes)

    def test_write_header_os(self):
        """
        Test writing out os to the header
        """
        os_field = 3
        os_field_bytes = (os_field).to_bytes(1, sys.byteorder)
        self.pigz_file._determine_operating_system = MagicMock()
        self.pigz_file._determine_operating_system.return_value = os_field
        self.pigz_file.output_file = MagicMock()

        self.pigz_file._write_header_os()

        self.pigz_file.output_file.write.assert_called_with(os_field_bytes)

    def test_process_chunk(self):
        """
        Test calling process chunk
        """
        chunk_num = 2
        chunk = b"What can I do? I can't take up and down like this no more, babe I need to find out where I am Before I reach the stars Yeah, before I step on Mars"  # noqa; pylint: disable=line-too-long
        compressed_chunk = b"Jamiroquai"
        self.pigz_file._last_chunk = chunk_num
        self.pigz_file._compress_chunk = MagicMock()
        self.pigz_file._compress_chunk.return_value = compressed_chunk
        self.pigz_file.chunk_queue = MagicMock()

        self.pigz_file._process_chunk(chunk_num, chunk)

        # Second arg is True since we've setup the test data as last chunk
        self.pigz_file._compress_chunk.assert_called_with(chunk, True)
        self.pigz_file.chunk_queue.put.assert_called_with(
            (chunk_num, chunk, compressed_chunk)
        )

    def test_clean_up(self):
        """
        Test that necessary cleanup tasks are completed
        """
        self.pigz_file.write_file_trailer = MagicMock()
        self.pigz_file.output_file = MagicMock()
        self.pigz_file._close_workers = MagicMock()

        self.pigz_file.clean_up()

        self.pigz_file.write_file_trailer.assert_called_once()
        self.pigz_file.output_file.flush.assert_called_once()
        self.pigz_file.output_file.close.assert_called_once()
        self.pigz_file._close_workers.assert_called_once()

    def test_write_file_trailer(self):
        """
        Test writing the file trailer
        """
        checksum = 8675309
        checksum_bytes = (checksum).to_bytes(4, sys.byteorder)
        input_size = 42069
        input_size_bytes = (input_size & 0xFFFFFFFF).to_bytes(4, sys.byteorder)

        self.pigz_file.output_file = MagicMock()
        self.pigz_file.checksum = checksum
        self.pigz_file.input_size = input_size

        self.pigz_file.write_file_trailer()

        self.pigz_file.output_file.write.assert_has_calls(
            [call(checksum_bytes), call(input_size_bytes)]
        )

    def test_determine_mtime_normal(self):
        """
        Test normal case of determing mtime of compression target
        """
        self.pigz_file.compression_target = MagicMock()
        mock_stat = Mock()
        mock_stat.st_mtime = 9440351000.0284
        with patch("os.stat", new=MagicMock(return_value=mock_stat)):
            mtime = self.pigz_file._determine_mtime()
            assert isinstance(mtime, int)
            self.assertEqual(mtime, 9440351000)

    def test_determine_mtime_exception(self):
        """
        Test exception case of determing mtime of compression target
        """
        self.pigz_file.compression_target = MagicMock()
        mock_time = 9440351000.0284

        def os_stat_throw_exception():
            raise Exception

        with patch("os.stat", new=os_stat_throw_exception):
            with patch("time.time", new=MagicMock(return_value=mock_time)):
                mtime = self.pigz_file._determine_mtime()
                assert isinstance(mtime, int)
                self.assertEqual(mtime, 9440351000)
