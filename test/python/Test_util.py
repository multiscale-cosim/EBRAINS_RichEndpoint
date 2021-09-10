import shutil
import tempfile
import os
import unittest
from pathlib import Path
# TODO: relative import instead of absolute import
from python.configuration_manager.utils import directory_utils


class TestUtilMakeDirectory(unittest.TestCase):
    """Tests the behavior of util function ``safe_makedir``."""
    @classmethod
    def __remove_directory_if_exists(cls, target_dir):
        try:
            shutil.rmtree(target_dir)
        except OSError as e:
            # directory does not exist
            pass

    def test_make_dir_exists(self):
        """Case: if the target directory already exists at the target location.
        It should return the path to that.
        """
        tmp_dir_os = os.path.join(tempfile.gettempdir(), "os_temp_dir")
        # remove the directory if it already exists
        self.__remove_directory_if_exists(tmp_dir_os)
        os.makedirs(tmp_dir_os)  # make the directory at target location
        # tests: whether the target directory already exists at target location
        self.assertTrue(os.path.isdir(tmp_dir_os))
        # tests: it should return the path to existing target directory
        self.assertEqual(Path(tmp_dir_os), Path(directory_utils.safe_makedir(tmp_dir_os)))
        shutil.rmtree(tmp_dir_os)  # clean up

    def test_make_new_dir_not_exists(self):
        """Case: if the target directory does not exist at the target location.
        It should create and return the path to that.
        """
        tmp_target_dir = os.path.join(tempfile.gettempdir(), "temp_target_dir")
        # remove the directory if it already exists
        self.__remove_directory_if_exists(tmp_target_dir)
        directory_utils.safe_makedir(tmp_target_dir)  # make a new directory
        # tests: whether the directory is created
        self.assertTrue(os.path.isdir(tmp_target_dir))
        # tests: it should return the path to the created target directory
        self.assertEqual(Path(tmp_target_dir), Path(directory_utils.safe_makedir(tmp_target_dir)))
        shutil.rmtree(tmp_target_dir)  # clean up

    def test_file_exists_same_name_location(self):
        """Case: if there exists a file at the target location with
        the same name. It should raise an exception.
        """
        # create a temp file
        fd, file_path = tempfile.mkstemp()
        # Test: it should raise an exception when trying to make a
        # directory with the same name as already existing file
        with self.assertRaises(Exception):
            directory_utils.safe_makedir(file_path)
        # clean up
        os.close(fd)  # close the file descriptor
        os.remove(file_path)  # delete the file


if __name__ == '__main__':
    unittest.main()
