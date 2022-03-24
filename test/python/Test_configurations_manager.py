import unittest
import os
import logging
import shutil
import glob
from pathlib import Path
from python.configuration_manager.configurations_manager import ConfigurationsManager
from python.configuration_manager.default_directories_enum import DefaultDirectories


class ConfigurationsManagerTest(unittest.TestCase):
    """Tests the behavior of ConfigurationsManager class."""
    @classmethod
    def setUpClass(cls) -> None:
        """set-up the common resources."""
        cls.configurations_manager = ConfigurationsManager()
        # may be better to use mock
        # a workaround for now
        # find xml file for the settings
        root = Path(__file__).parent.parent.parent
        files = glob.glob(os.path.join(os.path.join(root, '**/global_settings.xml')), recursive=True)
        try:
            cls.settings_file = files[0]
        except IndexError as e:
            raise e # TODO: add fallback workaround

    @classmethod
    def tearDownClass(cls) -> None:
        """clean up the common resources."""
        # TODO: clean up -- directories (or may be using mock an idea)
        #   e.g shutil.rmtree(cls.configurations_manager.get_directory('output'))
        # to prevent the side effects such as memory leaks
        del cls.configurations_manager

    def test_make_directory(self):
        """Case: whether a directory is successfully created."""
        # make a tests directory
        temp_dir = self.configurations_manager.make_directory('temp_dir', '')
        # tests: whether the directory is successfully created
        self.assertTrue(os.path.isdir(temp_dir))
        shutil.rmtree(temp_dir)  # clean up

    def test_make_directory_location_correctness(self):
        """Case: whether the directory is created at the target location."""
        # make a tests directory
        temp_dir = self.configurations_manager.make_directory('temp_dir_location_test', '')
        # tests: whether the location is correct
        temp_dir_path = Path(self.configurations_manager.get_directory('temp_dir_location_test'))
        self.assertTrue(Path.exists(temp_dir_path))
        shutil.rmtree(temp_dir)  # clean up

    def test_get_directory_exists(self, target_directory=None):
        """Case: the target_directory exists.
        It should return the path to that."""
        if target_directory is None:
            self.configurations_manager.setup_default_directories('')
            target_directory = DefaultDirectories.OUTPUT # default directory
        self.assertTrue(os.path.isdir(self.configurations_manager.
                                      get_directory(target_directory)))

    def test_get_directory_not_exists(self):
        """Case: the target_directory does not exist.
        It should raise an exception."""
        target_dir = 'not_exists'
        # tests: it should raise an exception if directory does not exist
        with self.assertRaises(Exception) as context:
            self.configurations_manager.get_directory(target_dir)
        self.assertTrue('directory not found' in str(context.exception))

    def test_get_component_configuration_settings_exists(self):
        """Case: the target ``component configuration settings`` exists.
        It should return the configuration settings for that."""
        target_component_configuration_settings = 'log_configurations'
        # Test: it returns the configurations settings that exist for the target component
        self.assertIsNotNone(self.configurations_manager.
                             get_configuration_settings(target_component_configuration_settings, self.settings_file))

    def test_get_component_configuration_settings_data_type_correctness(self):
        """Case: the return data type for the existing target
         configuration settings is 'correct'."""
        target_component_configuration_settings = 'log_configurations'
        target_data_type = dict
        # Test: it returns the correct data type
        self.assertIsInstance(self.configurations_manager.
                              get_configuration_settings(target_component_configuration_settings,
                                                         self.settings_file), target_data_type)

    def test_get_component_configuration_settings_not_exists(self):
        """Case: the target ``component configuration settings``
        does not exist. It should raise an exception."""
        target_component = 'not_exists'
        # Test: it should raise an 'LookupError' exception when the
        # target component configuration settings do not exist.
        with self.assertRaises(LookupError) as context:
            self.configurations_manager.get_configuration_settings(target_component, self.settings_file)
        # Test: the captured exception is the one that is raised.
        self.assertTrue("configuration settings not found!" in str(context.exception))

    def test_load_log_configurations(self):
        """Case: the logger is 'properly' configured. It should return
        the instance of Logging.Logger class with user defined name and
         settings, and should emit the logs at the user specified levels."""
        test_logger_name = __name__
        target_component = 'log_configurations'
        self.configurations_manager.setup_default_directories('')
        log_configurations = self.configurations_manager.get_configuration_settings(target_component, self.settings_file)
        with self.assertLogs(__name__, level='INFO') as context:
            logger = self.configurations_manager.load_log_configurations(test_logger_name, log_configurations)
            # tests: whether the logger is created
            self.assertIsNotNone(logger)
            # tests: whether the logger is an instance of class ``Logger``
            self.assertIsInstance(logger, logging.Logger)
            # tests: whether the logger is created with user defined name
            self.assertEqual(logger.name, test_logger_name)
            # emit two tests log messages
            logger.info("TEST INFO log")
            logger.error("TEST ERROR log")
        # tests: whether all log messages are emitted successfully
        self.assertEqual(len(context.records), 2)
        # tests: correctness of the log messages i.e. whether the captured log
        # messages are the ones that were emitted.
        self.assertEqual(context.records[0].getMessage(), "TEST INFO log")
        self.assertEqual(context.records[1].getMessage(), "TEST ERROR log")
        # tests: correctness of the level of emitted log messages
        self.assertEqual(context.records[0].levelno, logging.INFO)
        self.assertEqual(context.records[1].levelno, logging.ERROR)


if __name__ == '__main__':
    unittest.main()
