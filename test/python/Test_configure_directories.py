import unittest
import os
import shutil
from pathlib import Path
# TODO: relative import instead of absolute import
from python.configuration_manager.directories_manager import DirectoriesManager
from python.configuration_manager.default_directories_enum import DefaultDirectories


class TestDirectoriesManager(unittest.TestCase):
    """ Tests the behavior of DirectoriesManager class."""
    @classmethod
    def setUpClass(cls):
        """set-up the common resources."""
        cls.directory_manager = DirectoriesManager()

    @classmethod
    def tearDownClass(cls):
        """clean-up common resources."""
        # clean up -- directories
        shutil.rmtree(cls.directory_manager.get_directory(DefaultDirectories.OUTPUT))
        # clean up -- to prevent the side effects such as memory leaks
        del cls.directory_manager

    def test_singleton(self):
        """Case: only a single instance of ```DirectoriesManager```
        class exists."""
        directory_manager_new = DirectoriesManager()
        # tests: whether it refers to the same instance
        self.assertEqual(directory_manager_new, self.directory_manager)
        # clean up -- to prevent the side effects such as memory leaks
        del directory_manager_new

    def test_get_directory_exists(self, target_directory=None):
        """Case: the target exists. It should return the path to that."""
        if target_directory is None:
            self.directory_manager.setup_default_directories('')
            target_directory = DefaultDirectories.OUTPUT # default directory
        self.assertTrue(os.path.isdir(self.directory_manager.
                                      get_directory(target_directory)))

    def test_get_directory_not_exists(self):
        """Case: the ``target_directory`` does not exist.
        It should raise an exception."""
        target_dir = 'not_exists'
        # tests: it should raise an exception if directory does not exist
        with self.assertRaises(Exception) as context:
            self.directory_manager.get_directory(target_dir)
        self.assertTrue('directory not found' in str(context.exception))

    def test_default_directories_created(self):
        """Case: whether the default directories are created."""
        self.directory_manager.setup_default_directories('')
        self.test_get_directory_exists(DefaultDirectories.OUTPUT)
        self.test_get_directory_exists(DefaultDirectories.LOGS)
        self.test_get_directory_exists(DefaultDirectories.RESULTS)
        self.test_get_directory_exists(DefaultDirectories.FIGURES)
        self.test_get_directory_exists(DefaultDirectories.MONITORING_DATA)

    def test_make_directory(self):
        """Case: whether a directory is successfully created.
        It should return the path to that."""
        temp_dir = self.directory_manager.make_directory('temp_dir', '')
        self.assertTrue(os.path.isdir(temp_dir))
        shutil.rmtree(temp_dir)  # clean up

    def test_make_directory_location_correctness(self):
        """Case: whether the directory is created at the target location."""
        # make a tests directory
        temp_dir = self.directory_manager.make_directory('temp_dir_location_test', '')
        # tests: whether the location is correct
        temp_dir_path = Path(self.directory_manager.get_directory('temp_dir_location_test'))
        self.assertTrue(Path.exists(temp_dir_path))
        shutil.rmtree(temp_dir)  # clean up


if __name__ == '__main__':
    unittest.main()