"""
Unit tests for Pigz Python
"""
import unittest

import pigz_python.pigz_python as pigz_python


class TestPigzPython(unittest.TestCase):
    """ Unit tests for PigzPython class """

    def test_determine_extra_flags_max_compression(self):
        """ Ensure appropriate XFL or eXtra FLags value is returned for max compression values """
        compression_levels = [9, 10, 11, 12]
        expected_xfl = 2
        for value in compression_levels:
            xfl = pigz_python.PigzFile._determine_extra_flags(  # pylint: disable=protected-access
                value
            )
            self.assertEqual(xfl, expected_xfl)
