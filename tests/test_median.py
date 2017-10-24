import unittest
from digest.math import median


class TestMath(unittest.TestCase):
    """
    Unit tests for math module
    """
    def test_median(self):
        assert median([1, 2, 3]) == 2
        assert median([1, 3, 2]) == 2
        assert median([1, 2]) == 1.5
