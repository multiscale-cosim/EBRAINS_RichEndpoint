import unittest
import os

class CythonTest(unittest.TestCase):
    """

    """
    def setUp(self):
        pass

    def test_import(self):
        """
        only import the lib
        """
        import cython_lib

    def test_basic_call(self):
        import cython_lib

        self.assertEqual(cython_lib.accelerated.squared_simple(2),
                         4.0)



if __name__ == "__main__":
    unittest.main()