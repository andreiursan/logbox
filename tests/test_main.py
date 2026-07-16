import unittest

from logbox.__main__ import main


class TestMain(unittest.TestCase):
    def test_main_runs(self):
        main()


if __name__ == "__main__":
    unittest.main()
