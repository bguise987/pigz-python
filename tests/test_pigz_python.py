"""
Unit tests for Pigz Python
"""
import unittest

import pigz_python.pigz_python as pigz_python


class TestPigzPython(unittest.TestCase):
    """ Unit tests for PigzPython class """

    def test_determine_extra_flags_max_compression(self):
        compression_level = 9
        expected_xfl = 2
        xfl = pigz_python.PigzFile._determine_extra_flags(compression_level)
        self.assertEqual(xfl, expected_xfl)
