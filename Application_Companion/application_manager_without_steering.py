# ------------------------------------------------------------------------------
#  Copyright 2020 Forschungszentrum Jülich GmbH and Aix-Marseille Université
# "Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements; and to You under the Apache License,
# Version 2.0. "
#
# Forschungszentrum Jülich
# Institute: Institute for Advanced Simulation (IAS)
# Section: Jülich Supercomputing Centre (JSC)
# Division: High Performance Computing in Neuroscience
# Laboratory: Simulation Laboratory Neuroscience
# Team: Multi-scale Simulation and Design
# ------------------------------------------------------------------------------
import multiprocessing
import os
import subprocess
import time
import signal
import fcntl

from EBRAINS_RichEndpoint.Application_Companion.signal_manager import SignalManager
from EBRAINS_ConfigManager.global_configurations_manager.xml_parsers.default_directories_enum import DefaultDirectories
from EBRAINS_RichEndpoint.Application_Companion.resource_usage_monitor import ResourceUsageMonitor
from EBRAINS_RichEndpoint.Application_Companion.common_enums import Response
from EBRAINS_RichEndpoint.Application_Companion.db_manager_file import DBManagerFile
from EBRAINS_RichEndpoint.Application_Companion.affinity_manager import AffinityManager


class ApplicationManager(multiprocessing.Process):
    """
    i).  Executes the action (application) as a child process,
    ii). Monitors its resource usage, and
    iii) Save the monitoring data to files.
    """
    def __init__(self,
                 # Dependency Injection for setting up the uniform log settings
                 log_settings, configurations_manager,
                 # actions (applications) to be launched
                 actions,
                 # proxy to shared queue for sending the responses
                 am_response_queue,
                 # flag to enable/disable resource usage monitoring
                 enable_resource_usage_monitoring=True):
        multiprocessing.Process.__init__(self)
        self._log_settings = log_settings
        self._configurations_manager = configurations_manager
        self.__logger = self._configurations_manager.load_log_configurations(
                                        name=__name__,
                                        log_configurations=self._log_settings)
        self.__application_manager_response_queue = am_response_queue
        self.__actions = actions
        self.__actions_id = None
        # set up signal handeling for such as CTRL+C
        self.__signal_manager = SignalManager(
                                  log_settings=log_settings,
                                  configurations_manager=configurations_manager
                                  )
        signal.signal(signal.SIGINT,
                      self.__signal_manager.interrupt_signal_handler
                      )
        signal.signal(signal.SIGTERM,
                      self.__signal_manager.kill_signal_handler
                      )
        # event to record that SIGINT signal is captured
        self.__stop_event = self.__signal_manager.shut_down_event
        # event to record that SIGTERM signal is captured
        self.__kill_event = self.__signal_manager.kill_event
        self.__is_monitoring_enabled = enable_resource_usage_monitoring
        # if monitoring is enabled, then initialize monitoring data handler
        if self.__is_monitoring_enabled:
            # NOTE monitoring data is dumped in to JSON files
            # initialize JSON files handler
            self.__db_manager_file = DBManagerFile(
                self._log_settings, self._configurations_manager)
        # initialize AffinityManager for handling affinity settings
        self.__affinity_manager = AffinityManager(
            self._log_settings, self._configurations_manager)
        # bind the Application Manager to a single core (i.e core 1) only
        # so not to interrupt the execution of the action (application)
        self.__bind_to_cpu = [0]  # TODO: configure it from configurations file
        # get available CPU cores to execute the application
        self.__legitimate_cpu_cores =\
            self.__affinity_manager.available_cpu_cores
        self.__popen_process = None
        self.__resource_usage_monitor = None
        self.__logger.debug("Application Manager is initialized.")

    def __set_affinity(self, pid):
        '''
        helper function to bind the application to a set of CPUs.

        Parameters
        ----------
        pid : int
            process PID

        Returns
        ------
        int
            return code indicating whether or not the affinity is set
        '''
        # NOTE: core 1 is bound to the application manager itself,
        # rest are bound to the main application.
        bind_with_cores = list(range(self.__bind_to_cpu[0]+1,
                                     self.__legitimate_cpu_cores))
        return self.__affinity_manager.set_affinity(pid, bind_with_cores)

    def __launch_application(self, application):
        '''
        helper function to launch the application with arguments provided.

        Parameters
        ----------
        application : str
            application to be launched

        Returns
        ------
        int
            return code indicating whether or not the application is launched
        '''
        # Turn-off output buffering for the child process
        os.environ['PYTHONUNBUFFERED'] = "1"
        # 1. run the application
        try:
            # self.__popen_process = subprocess.Popen(
            #                         [application, application_args],
            #                         stdin=None,
            #                         stdout=subprocess.PIPE,
            #                         stderr=subprocess.PIPE,
            #                         shell=False)
            self.__popen_process = subprocess.Popen(
                                    application,
                                    stdin=subprocess.PIPE,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE,
                                    shell=False)
            # let the application completes its setup for execution
            time.sleep(0.001)
        # Case a, launching is failed and raises an exception such as OSError
        # or ValueError
        except Exception:
            # log the exception with traceback
            self.__logger.exception(f'Could not run <{self.__actions_id}>.')
            # return with error to terminate loudly
            return Response.ERROR

        # Case b, application is launched.
        self.__logger.debug(f'<{self.__actions_id}> is launched.')

        # 2. set the affinity for the application
        if self.__set_affinity(self.__popen_process.pid) == Response.ERROR:
            # affinity could not be set, log exception with traceback
            try:
                # raise runtime exception
                raise(RuntimeError)
            except RuntimeError:
                # log the exception with traceback details
                self.__logger.exception(
                            self.__logger,
                            f'affinity could not be set for '
                            f'<{self.__actions_id}>:'
                            f'{self.__popen_process.pid}')
            # NOTE: application is launched with no defined affinity mask

        # 3. start resource usage monitoring, if enabled
        if self.__is_monitoring_enabled:
            # get affinity mask
            bind_with_cores = self.__affinity_manager.get_affinity(
                        self.__popen_process.pid)
            # initialize resource usage monitor
            self.__resource_usage_monitor = ResourceUsageMonitor(
                                                self._log_settings,
                                                self._configurations_manager,
                                                self.__popen_process.pid,
                                                bind_with_cores)
            # start monitoring
            # Case a, monitoring could not be started
            if self.__resource_usage_monitor.start_monitoring() ==\
                    Response.ERROR:
                try:
                    # raise runtime exception
                    raise(RuntimeError)
                except RuntimeError:
                    # log the exception with traceback details
                    self.__logger.exception(
                                self.__logger,
                                f'Could not start monitoring for '
                                f'<{self.__actions_id}>: '
                                f'{self.__popen_process.pid}')

        # Case b, monitoring is started
        self.__logger.debug("started monitoring the resource usage.")

        # Everything goes right
        self.__logger.info(f'<{self.__actions_id}> starts execution')
        self.__logger.debug(f"PID:{os.getpid()} is executing the action "
                            f"<{self.__actions_id}>, "
                            f"PID={self.__popen_process.pid}")
        return Response.OK

    def __stop_preemptory(self):
        '''helper function to terminate the application forcefully.'''
        if self.__kill_event.is_set() or self.__stop_event.is_set():
            self.__logger.info(f"going to signal "
                               f"PID={self.__popen_process.pid}"
                               f" to terminate.")
            self.__popen_process.terminate()
            time.sleep(0.001)  # let the Popen process be finished
            try:
                self.__popen_process.wait(timeout=1)
            except subprocess.TimeoutExpired:
                # Could not terminate the process,
                # send signal to forcefully kill it
                self.__logger.info(f"going to signal "
                                   f"PID={self.__popen_process.pid} "
                                   f"to forcefully quit.")
                # quit the process forcefully
                self.__popen_process.kill()

            # check whether the process finishes
            exit_status = self.__popen_process.poll()
            # Worst Case, process could not be terminated/killed
            if exit_status is None:
                try:
                    # raise runtime exception
                    raise(RuntimeError)
                except RuntimeError:
                    # log the exception with traceback details
                    self.__logger.exception(
                                            self.__logger,
                                            f"could not terminate the process "
                                            f"PID={self.__popen_process.pid}")
                # send response to terminate with error
                return Response.ERROR

            # Case, process is termianted/killed
            self.__logger.info(f"terminated PID={self.__popen_process.pid}"
                               f" exit_status={exit_status}")
            return Response.OK

    def __non_block_read(self, std_stream):
        '''
        helper function for reading from output/error stream of the process
        launched.
        '''
        fd = std_stream.fileno()
        fl = fcntl.fcntl(fd, fcntl.F_GETFL)
        fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)
        try:
            return std_stream.read()
        except Exception:
            self.__logger.exception(
                f'excpetion while reading from {std_stream}')
            return ''

    def __read_popen_pipes(self, application):
        '''
        helper function to read the outputs from the application.

        Parameters
        ----------
        application : str
            application which is launched


        Returns
        ------
        int
            return code indicating whether or not the outputs are read
        '''
        exit_status = None  # process exit status
        stdout_line = None  # to read from output stream
        stderr_line = None  # to read from error stream
        # read until the process is running
        while exit_status is None:
            try:
                # read from the application output stream
                stdout_line = self.__non_block_read(self.__popen_process.stdout)
                # log the output received from the application
                if stdout_line:
                    self.__logger.info(
                                f"action <{self.__actions_id}>: "
                                f"{stdout_line.strip().decode('utf-8')}")
                # read from the application error stream
                stderr_line = self.__non_block_read(self.__popen_process.stderr)
                # log the error reported by the application
                if stderr_line:
                    self.__logger.error(
                                f"{application}: "
                                f"{stderr_line.strip().decode('utf-8')}")

                # check if process is still running
                exit_status = self.__popen_process.poll()
                # Case, process is finished and there is nothing to read in the
                # stdout and stderr streams.
                if exit_status is not None and\
                        not stdout_line and not stderr_line:
                    # exit the loop
                    break

                # Otherwise, wait a bit to let the process proceed the execution
                time.sleep(0.1)
                # continue reading
                continue

            # just in case if process hangs on reading
            except KeyboardInterrupt:
                # log the exception with traceback
                self.__logger.exception(f"KeyboardInterrupt caught by: "
                                        f"action <{self.__actions_id}>")
                # terminate the application process preemptory
                if self.__stop_preemptory() == Response.ERROR:
                    # Case, process could not be terminated
                    self.__logger.error(f'could not terminate the action '
                                        f'<{self.__actions_id}>')
                # terminate reading loop with ERROR
                return Response.ERROR

        # everything goes right i.e. application finished execution and
        # outputs are read
        return Response.OK

    def __post_processing(self, exit_code):
        '''
        helper function to post process after application execution.

        Parameters
        ----------
        exit code : int
            exit code of the application

        Returns
        ------
        int
            return code indicating whether or not the post processing completes
        '''
        # conclude resource usage monitoring and save the data
        if self.__is_monitoring_enabled:
            # stop resource usage monitoring
            self.__resource_usage_monitor.keep_monitoring = False
            # retrieve resource usage statistics
            resources_usage_by_popen_process = self.__resource_usage_monitor.\
                get_resource_usage_stats(exit_code)
            self.__logger.debug(f"Resource Usage stats: "
                                f"{resources_usage_by_popen_process.items()}")
            # get directory to save the resource usage statistics
            try:
                metrics_output_directory = \
                    self._configurations_manager.get_directory(
                        DefaultDirectories.MONITORING_DATA)
                # exception raised, if default directory does not exist
            except KeyError:
                # create a new directory
                metrics_output_directory = \
                    self._configurations_manager.make_directory(
                        'Resource usage metrics', directory_path='AC results')

            # path to JSON file for dumping the monitoring data
            metrics_file = os.path.join(metrics_output_directory,
                                        f'pid_{self.__popen_process.pid}'
                                        '_resource_usage_metrics.json')
            # dump the monitoring data
            self.__db_manager_file.write(
                            metrics_file,
                            resources_usage_by_popen_process)
        return Response.OK

    def run(self):
        """
        starts Application Manager for
        i)   executing the application,
        ii)  monitoring its resource usage, and
        iii) reading the outputs from the application.
        """
        # flag to indicate if something goes wrong
        terminate_with_error = False
        # 1. set affinity
        pid = os.getpid()
        # Case a, affinity is not set
        if self.__affinity_manager.set_affinity(
                    pid, self.__bind_to_cpu) == Response.ERROR:
            try:
                # raise run time error exception
                raise RuntimeError
            except RuntimeError:
                # log the exception with traceback
                self.__logger.exception('Affinity could not be set.')

        # 2. fetch the application to be executed
        application = self.__actions.get('action')
        # 3. fetch the application parameters
        # application_args = self.__actions.get('args')
        # 4. fetch the action id
        self.__actions_id = self.__actions.get('action-id')
        self.__logger.debug(
            f'application:{application}, action_id:{self.__actions_id}')

        # 4. Launch application
        if self.__launch_application(application) == Response.ERROR:
            # Case a, could not launch the application
            try:
                # raise runtime exception
                raise(RuntimeError)
            except RuntimeError:
                # log the exception with traceback details
                self.__logger.exception('Launching is failed. Quitting!')
            # send error as response to Application Companion
            self.__application_manager_response_queue.put(Response.ERROR)
            # terminate with error
            return Response.ERROR
        # Case b, application is launched successfully
        self.__logger.debug('application is launched.')

        # 5. Read outputs from the application
        if self.__read_popen_pipes(application) == Response.ERROR:
            # Case a, could not read the outputs
            terminate_with_error = True
            try:
                # raise runtime exception
                raise(RuntimeError)
            except RuntimeError:
                # log the exception with traceback details
                self.__logger.exception('error reading output')
        # Case b, outputs are read successfully
        self.__logger.debug('outputs are read.')

        # 6. Check whether the application executed successfully
        # get the exit code of the application
        exit_code = self.__popen_process.poll()
        #  Case a, something went wrong
        if not exit_code == 0:
            terminate_with_error = True
            try:
                # raise runtime exception
                raise(RuntimeError)
            except RuntimeError:
                # log the exception with traceback details
                self.__logger.exception(
                                f"application <{application}> "
                                f" action: <{self.__actions_id}> execution "
                                f"went wrong, finished with rc = {exit_code}")
        # Case b, application executed successfully
        self.__logger.info(f'Action <{self.__actions_id}> finished properly.')

        # 7. post processing
        if self.__post_processing(exit_code) == Response.ERROR:
            terminate_with_error = True
            # Case a, Post processing fails
            try:
                # raise runtime exception
                raise(RuntimeError)
            except RuntimeError:
                # log the exception with traceback details
                self.__logger.exception('post processing fails.')
        # Case b, post processing is done
        self.__logger.info('post processing is done.')

        # 8. send response to Application Companion
        # Case a, something went wrong. Send ERROR as response.
        if terminate_with_error:
            self.__application_manager_response_queue.put(Response.ERROR)
            return Response.ERROR

        # Case b, everything  goes right. Send OK as response.
        self.__application_manager_response_queue.put(Response.OK)
        return Response.OK
