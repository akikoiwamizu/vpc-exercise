import unittest

class TestFunctions(unittest.TestCase):
    def test_circle_area2(self):
        assert abs(circle_area(2) - 12.56637) < 0.0001
    def test_circle_area0(self):
        assert circle_area(0) == 0

unittest.main(argv=[''], verbosity=2, exit=False)
