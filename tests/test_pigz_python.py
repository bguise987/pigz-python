"""
Unit tests for Pigz Python
"""
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import pigz_python.pigz_python as pigz_python

LOREM_IPSUM_FILE = "lorem_ipsum.txt"

# pylint: disable=protected-access
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
        expected_output = b"\n\xc9\xc8,V\x00\xa2D\x85\x92\xd4\xe2\x12\x85\xe2\x92\xa2\xcc\xbct\x00\x00\x00\x00\xff\xff"  # pylint: disable=line-too-long
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
