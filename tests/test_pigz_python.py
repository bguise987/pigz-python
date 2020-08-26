"""
Unit tests for Pigz Python
"""
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import pigz_python.pigz_python as pigz_python

LOREM_IPSUM_FILE = "lorem_ipsum.txt"


class TestPigzPython(unittest.TestCase):
    """ Unit tests for PigzPython class """

    def setUp(self):
        """
        Setup PigzFile instance for testing, using a mock file.
        """
        test_file = Path("tests", LOREM_IPSUM_FILE)
        self.pigz_file = pigz_python.PigzFile(test_file)

    def test_determine_operating_system_windows(self):
        """
        Test finding operating system on Windows
        """
        mock_sys_platform = "win32"
        expected_operating_system = 0
        with patch(
            "sys.platform", new_callable=MagicMock(return_value=mock_sys_platform)
        ):
            operating_system = (
                pigz_python.PigzFile._determine_operating_system()  # noqa; pylint: disable=protected-access
            )
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
            operating_system = (
                pigz_python.PigzFile._determine_operating_system()  # noqa; pylint: disable=protected-access
            )
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
            operating_system = (
                pigz_python.PigzFile._determine_operating_system()  # noqa; pylint: disable=protected-access
            )
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
            operating_system = (
                pigz_python.PigzFile._determine_operating_system()  # noqa; pylint: disable=protected-access
            )
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
            operating_system = (
                pigz_python.PigzFile._determine_operating_system()  # noqa; pylint: disable=protected-access
            )
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
            operating_system = (
                pigz_python.PigzFile._determine_operating_system()  # noqa; pylint: disable=protected-access
            )
        self.assertEqual(operating_system, expected_operating_system)

    def test_determine_extra_flags_max_compression(self):
        """
        Ensure appropriate XFL or eXtra FLags value is returned for
        max compression values
        """
        compression_levels = [9, 10, 11, 12]
        expected_xfl = 2
        for value in compression_levels:
            xfl = pigz_python.PigzFile._determine_extra_flags(  # noqa; pylint: disable=protected-access
                value
            )
            self.assertEqual(xfl, expected_xfl)

    def test_determine_extra_flags_min_compression(self):
        """
        Ensure appropriate XFL or eXtra FLags value is returned for
        min compression value
        """
        compression_level = 1
        expected_xfl = 4
        xfl = pigz_python.PigzFile._determine_extra_flags(  # noqa; pylint: disable=protected-access
            compression_level
        )
        self.assertEqual(xfl, expected_xfl)

    def test_determine_extra_flags_default_compression(self):
        """
        Ensure appropriate XFL or eXtra FLags value is returned for values
        not max or min
        """
        compression_levels = [0, 2, 3, 4, 5, 6, 7, 8]
        expected_xfl = 0
        for value in compression_levels:
            xfl = pigz_python.PigzFile._determine_extra_flags(  # noqa; pylint: disable=protected-access
                value
            )
            self.assertEqual(xfl, expected_xfl)

    def test_set_output_filename(self):
        """
        Ensure output filenames are appropriate
        """
        expected_output_filename = f"{LOREM_IPSUM_FILE}.gz"
        self.pigz_file._set_output_filename()
        self.assertEqual(self.pigz_file.output_filename, expected_output_filename)
