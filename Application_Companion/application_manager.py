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
import ast

from EBRAINS_RichEndpoint.Application_Companion.signal_manager import SignalManager
from EBRAINS_ConfigManager.global_configurations_manager.xml_parsers.default_directories_enum import DefaultDirectories
from EBRAINS_RichEndpoint.Application_Companion.resource_usage_monitor import ResourceUsageMonitor
from EBRAINS_RichEndpoint.Application_Companion.common_enums import INTEGRATED_SIMULATOR_APPLICATION as SIMULATOR
from EBRAINS_RichEndpoint.Application_Companion.common_enums import Response
from EBRAINS_RichEndpoint.orchestrator.communicator_queue import CommunicatorQueue
from EBRAINS_RichEndpoint.Application_Companion.common_enums import SteeringCommands
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
                 # proxy to shared queue for receiving the commands
                 am_in_queue,
                 # proxy to shared queue for sending the responses
                 am_out_queue,
                 # flag to enable/disable resource usage monitoring
                 enable_resource_usage_monitoring=True):
        multiprocessing.Process.__init__(self)
        self._log_settings = log_settings
        self._configurations_manager = configurations_manager
        self.__logger = self._configurations_manager.load_log_configurations(
            name=__name__,
            log_configurations=self._log_settings)
        self.__application_manager_in_queue = am_in_queue
        self.__application_manager_out_queue = am_out_queue
        self.__actions = actions
        self.__actions_id = None
        self.__application = None
        # set up signal handling for such as CTRL+C
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
        # bind the Application Manager to a single core (i.e. core 1) only
        # so not to interrupt the execution of the action (application)
        self.__bind_to_cpu = [0]  # TODO: configure it from configurations file
        # get available CPU cores to execute the application
        self.__legitimate_cpu_cores = \
            self.__affinity_manager.available_cpu_cores
        self.__communicator = None
        self.__popen_process = None
        self.__resource_usage_monitor = None
        self.__exit_status = None
        self.__local_minimum_step_size = {}
        self.__logger.debug("Application Manager is initialized.")

    def __set_affinity(self, pid):
        """
        helper function to bind the application to a set of CPUs.

        Parameters
        ----------
        pid : int
            process PID

        Returns
        ------
        int
            return code indicating whether the affinity is set
        """
        # NOTE: core 1 is bound to the application manager itself,
        # rest are bound to the main application.
        bind_with_cores = list(range(self.__bind_to_cpu[0] + 1,
                                     self.__legitimate_cpu_cores))
        return self.__affinity_manager.set_affinity(pid, bind_with_cores)

    def __launch_application(self, application):
        """
        helper function to launch the application with arguments provided.

        Parameters
        ----------
        application : str
            application to be launched

        Returns
        ------
        int
            return code indicating whether the application is launched
        """

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
            # NOTE: application is launched with no defined affinity mask
            try:
                # raise runtime exception
                raise (RuntimeError)
            except RuntimeError:
                # log the exception with traceback details
                self.__logger.exception(
                    self.__logger,
                    f'affinity could not be set for '
                    f'<{self.__actions_id}>:'
                    f'{self.__popen_process.pid}')

        # # 3. start resource usage monitoring, if enabled
        # if self.__is_monitoring_enabled:
        #     if self.__start_resource_usage_monitoring() == Response.ERROR:
        #         # monitoring could not be started, a relevant exception is
        #         # already logged with traceback
        #         return Response.ERROR

        # Otherwise, everything goes right
        self.__logger.info(f'<{self.__actions_id}> starts execution')
        self.__logger.debug(f"PID:{os.getpid()} is executing the action "
                            f"<{self.__actions_id}>, "
                            f"PID={self.__popen_process.pid}")
        return Response.OK

    def __start_resource_usage_monitoring(self, pid):
        """starts monitoring of the resources' usage by the application."""
        # get affinity mask
        bind_with_cores = self.__affinity_manager.get_affinity(
            self.__popen_process.pid)
        # initialize resource usage monitor
        self.__resource_usage_monitor = ResourceUsageMonitor(
            self._log_settings,
            self._configurations_manager,
            # self.__popen_process.pid,
            pid,
            bind_with_cores)
        # start monitoring
        # Case a, monitoring could not be started
        if self.__resource_usage_monitor.start_monitoring() == \
                Response.ERROR:
            try:
                # raise runtime exception
                raise (RuntimeError)
            except RuntimeError:
                # log the exception with traceback details
                self.__logger.exception(
                    self.__logger,
                    f'Could not start monitoring for '
                    f'<{self.__actions_id}>: '
                    # f'{self.__popen_process.pid}')
                    f'{pid}')
                return Response.ERROR

        # Case b, monitoring is started                    
        self.__logger.debug("started monitoring the resource usage.")
        return Response.OK

    def __stop_preemptory(self):
        """helper function to terminate the application forcefully."""
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
                    raise (RuntimeError)
                except RuntimeError:
                    # log the exception with traceback details
                    self.__logger.exception(
                        self.__logger,
                        f"could not terminate the process "
                        f"PID={self.__popen_process.pid}")
                # send response to terminate with error
                return Response.ERROR

            # Case, process is terminated/killed
            self.__logger.info(f"terminated PID={self.__popen_process.pid}"
                               f" exit_status={exit_status}")
            return Response.OK

    def __non_block_read(self, std_stream):
        """
        helper function for reading from output/error stream of the process
        launched.
        """
        fd = std_stream.fileno()
        fl = fcntl.fcntl(fd, fcntl.F_GETFL)
        fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)
        try:
            return std_stream.read()
        except Exception:
            self.__logger.exception(
                f'exception while reading from {std_stream}')
            return ''

    def __convert_string_to_dictionary(self, lines):
        """
        finds and extracts the local minimum step size information from
        std_out stream, and converts it to a dictionary.

        Parameters
        ----------
        lines : str
            output received from the application
        """
        # STEP 1. find the index of local minimum step size information in the
        # output received from application
        index = lines.find(SIMULATOR.PID.name)

        # STEP 2. covert string to dictionary

        # NOTE as per protocol, the local minimum step size is received as a
        # response of INIT command
        # For now, it is received via (stdin) PIPE as a string in the
        # following format:
        # {'PID': '<pid>', 'LOCAL_MINIMUM_STEP_SIZE': '0.05'}
        # so the index of curly bracket {'PID'... is index-2, which is needed
        # to convert it into dictionary.
        try:
            self.__local_minimum_step_size = ast.literal_eval(lines[index - 2:])
            return Response.OK
        except Exception:
            # Could not convert string into dict
            # log the exception with traceback and return with error
            self.__logger.exception(f'could not convert {lines[index - 2:]} into'
                                    f' the dictionary.')
            return Response.ERROR

    def __read_popen_pipes(self, application):
        """
        helper function to read the outputs from the application.

        Parameters
        ----------
        application : str
            application which is launched


        Returns
        ------
        int
            return code indicating whether the outputs are read
        """
        stdout_line = None  # to read from output stream
        stderr_line = None  # to read from error stream
        # read until the process is running
        while self.__exit_status is None:
            try:
                # read from the application output stream
                stdout_line = self.__non_block_read(self.__popen_process.stdout)
                # log the output received from the application
                if stdout_line:
                    decoded_lines = stdout_line.strip().decode('utf-8')
                    self.__logger.info(
                        f"action <{self.__actions_id}>: "
                        f"{decoded_lines}")
                    # get local minimum step size received from the application
                    # as a response to INIT command

                    if SIMULATOR.LOCAL_MINIMUM_STEP_SIZE.name in decoded_lines:
                        if self.__convert_string_to_dictionary(decoded_lines) == \
                                Response.ERROR:
                            # Case a. Local minimum step size could not be
                            # determined, terminate the execution with error
                            # NOTE an exception with traceback is already
                            # logged
                            self.__stop_preemptory()
                            return Response.ERROR

                        # Case b. All information (received as a response to
                        # INIT command) is read from PIPES, now execute other
                        # steering commands
                        break

                # read from the application error stream
                stderr_line = self.__non_block_read(self.__popen_process.stderr)
                # log the error reported by the application
                if stderr_line:
                    self.__logger.error(
                        f"{application}: "
                        f"{stderr_line.strip().decode('utf-8')}")

                # check if process is still running
                self.__exit_status = self.__popen_process.poll()
                # Case, process is finished and there is nothing to read in the
                # stdout and stderr streams.
                if self.__exit_status is not None and \
                        not stdout_line and not stderr_line:
                    # stop reading from PIPES and exit the loop
                    break

                # Otherwise, wait a bit to let the process proceed the execution
                time.sleep(1)
                # continue reading
                continue

            # just in case if process hangs on reading
            except KeyboardInterrupt:
                # log the exception with traceback
                self.__logger.exception(f"KeyboardInterrupt caught by: "
                                        f"action <{self.__actions_id}>")
                # terminate the application process peremptory
                if self.__stop_preemptory() == Response.ERROR:
                    # Case, process could not be terminated
                    self.__logger.error(f'could not terminate the action '
                                        f'<{self.__actions_id}>')
                # terminate reading loop with ERROR
                return Response.ERROR

        # everything goes right i.e. application finished execution and
        # outputs are read
        return Response.OK

    def __conclude_resource_usage_monitoring(self):
        """
        1. Stops resource usage monitoring
        2. Dumps the monitoring data to a JSON file.
        """
        # stop resource usage monitoring
        self.__resource_usage_monitor.keep_monitoring = False
        # retrieve resource usage statistics
        resources_usage_by_popen_process = self.__resource_usage_monitor. \
            get_resource_usage_stats(self.__exit_status)
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

    def __post_processing(self):
        """
        helper function to post process after application execution.

        Parameters
        ----------
        exit code : int
            exit code of the application

        Returns
        ------
        int
            return code indicating whether the post-processing completes
        """
        # conclude resource usage monitoring and save the data
        if self.__is_monitoring_enabled:
            if self.__conclude_resource_usage_monitoring() == Response.ERROR:
                # Case a, Post processing fails
                try:
                    # raise runtime exception
                    raise RuntimeError
                except RuntimeError:
                    # log the exception with traceback details
                    self.__logger.exception('post processing fails.')
                return Response.ERROR

        # Case b, post-processing is done
        self.__logger.info('post processing is done.')
        return Response.OK

    def __send_command_to_application(self, command):
        """
        helper function to send Steering Commands to application.

        Parameters
        ----------

        command : SteeringCommand
            steering command to be sent to the application
        """
        self.__logger.debug(f'sending command: {command} to application...')
        self.__popen_process.stdin.write(f'{command}\n'.encode())
        self.__popen_process.stdin.flush()

    def __send_response_to_application_companion(self, response):
        """
        sends response to Orchestrator as a result of Steering Command
        execution.

        Parameters
        ----------

        response : ...
            response to be sent to Orchestrator

        Returns
        ------
            return code as int
        """
        self.__logger.info(f"sending {response} to Application Companion.")
        return self.__communicator.send(
            response, self.__application_manager_out_queue)

    def __execute_init_command(self):
        """
        1. Executes INIT steering command by launching the application.

        NOTE INIT of application (initialization of buffers, network,
        local minimum delay, etc.) is a system action and is done implicitly
        with launching.

        2. Receives the local step size from the simulator and sends it to the
        Orchestrator via Application Companion.
        """
        # 1. Launch application
        if self.__launch_application(self.__application) == Response.ERROR:
            # Case a, could not launch the application
            try:
                # raise runtime exception
                raise RuntimeError
            except RuntimeError:
                # log the exception with traceback details
                self.__logger.exception('Launching is failed. Quitting!')
            # send error as response to Application Companion
            self.__send_response_to_application_companion(Response.ERROR)
            # terminate with error
            return Response.ERROR

        # Case b, application is launched successfully
        self.__logger.debug('application is launched.')

        # 2. Get minimum local step size from application and send to AC

        # NOTE local minimum step size is sent by the simulators only.
        # Therefore, filter InterscaleHub when receiving output via (stdin)
        # PIPE. Otherwise, it would hang on reading from PIPE.
        # if self.__actions_id == 'action_004' or self.__actions_id == 'action_010':  # TODO hardcoded action id
        if self.__read_popen_pipes(self.__application) == Response.ERROR:
            # Case a, could not read the outputs
            try:
                # raise runtime exception
                raise RuntimeError
            except RuntimeError:
                # log the exception with traceback details
                self.__logger.exception('error reading output')
            # send error as response to Application Companion
            self.__send_response_to_application_companion(Response.ERROR)
            # terminate with error
            return Response.ERROR

            # Case b, outputs are read successfully
        self.__logger.debug('outputs are read.')

        # xxx start resource usage monitoring of the application
        # 3. start resource usage monitoring, if enabled
        if self.__is_monitoring_enabled:
            # pid = self.__local_minimum_step_size[SIMULATOR.PID.name]
            pid = self.__local_minimum_step_size.get("PID")
            self.__logger.info(f"starting monitoring for {pid}")
            if self.__start_resource_usage_monitoring(pid) == Response.ERROR:
                # monitoring could not be started, a relevant exception is
                # already logged with traceback
                return Response.ERROR

        # 3. send local minimum step size as a response to Application
        # Companion
        self.__logger.debug(f'outputs are read from {self.__actions_id}: {self.__local_minimum_step_size}')
        self.__send_response_to_application_companion(
            self.__local_minimum_step_size)
        return Response.OK

    def __execute_start_command(self):
        """
        1. Executes START steering command by sending it to applications.
        2. Receives output from the application.
        3. Does post-processing after the execution of applications.
        """
        # 1. send command to application
        self.__send_command_to_application(SteeringCommands.START.name)

        # 2. Read outputs from the application
        if self.__read_popen_pipes(self.__application) == Response.ERROR:
            # Case a, could not read the outputs
            try:
                # raise runtime exception
                raise RuntimeError
            except RuntimeError:
                # log the exception with traceback details
                self.__logger.exception('error reading output')
            # send error as response to Application Companion
            self.__send_response_to_application_companion(Response.ERROR)
            # terminate with error
            return Response.ERROR

        # Case b, outputs are read successfully
        self.__logger.debug('outputs are read.')

        # 3. process is finished, do post-processing (e.g. stop monitoring etc)
        if self.__post_processing() == Response.ERROR:
            # Case a, Post processing fails
            try:
                # raise runtime exception
                raise RuntimeError
            except RuntimeError:
                # log the exception with traceback details
                self.__logger.exception('post processing fails.')
            # send error as response to Application Companion
            self.__send_response_to_application_companion(Response.ERROR)
            # terminate with error
            return Response.ERROR

        # Case b, post-processing is done
        self.__logger.info('post processing is done.')

        # send response to Application Companion
        self.__send_response_to_application_companion(Response.OK)
        return Response.OK

    def __execute_end_command(self):
        """
        Checks the exit status of the application.

        NOTE later add other functionality here such as post data analysis etc.
        """
        # Check whether the application executed successfully
        self.__exit_status = self.__popen_process.poll()
        #  Case a, something went wrong during application execution
        if not self.__exit_status == 0:
            try:
                # raise runtime exception
                raise RuntimeError
            except RuntimeError:
                # log the exception with traceback details
                self.__logger.exception(
                    f"application <{self.__application}> "
                    f" action: <{self.__actions_id}> execution "
                    f"went wrong, finished with rc = {self.__exit_status}")
            # send error as a response to Application Companion
            self.__send_response_to_application_companion(Response.ERROR)
            # terminate with error
            return Response.ERROR

        # Case b, application executed successfully
        self.__logger.info(f'Action <{self.__actions_id}> finished properly.')
        self.__send_response_to_application_companion(Response.OK)
        return Response.OK

    def __fetch_and_execute_steering_commands(self):
        """
        Main loop to fetch and execute the steering commands.
        The loop terminates either by normally or forcefully i.e.:
        i)  Normally: receiving the steering command END, or by
        ii) Forcefully: receiving the FATAL command from Orchestrator.
        """
        # create a dictionary of choices for the steering commands and
        # their corresponding executions
        command_execution_choices = {
            SteeringCommands.INIT: self.__execute_init_command,
            SteeringCommands.START: self.__execute_start_command,
            SteeringCommands.END: self.__execute_end_command,
        }

        # loop for executing and fetching the steering commands
        while True:
            # 1. fetch the Steering Command
            current_steering_command = self.__communicator.receive(
                self.__application_manager_in_queue
            )
            self.__logger.debug(f"got the command {current_steering_command}")
            # 2. execute the current steering command
            if command_execution_choices[current_steering_command]() == \
                    Response.ERROR:
                # something went wrong, terminate with error
                # NOTE a relevant exception is already logged with traceback
                # detail by the target function
                return Response.ERROR

            # 3(a). END command is already executed, finish execution as normal
            if current_steering_command == SteeringCommands.END:
                self.__logger.debug("Concluding execution.")
                # finish execution as normal
                break

            # 3 (b). Otherwise, keep fetching/executing the steering commands
            continue

    def __pre_processing(self):
        """
        Does pre-processing such as set affinity for its won self, set up
        Communicator, get application to be executed, etc.
        """
        self.__logger.debug('pre-processing starts...')
        pid = os.getpid()  # get its own PID

        # 1. bind itself with a user defined single CPU core e.g. CPU core 1
        if self.__affinity_manager.set_affinity(
                pid, self.__bind_to_cpu) == Response.ERROR:
            # Case a, affinity is not set
            # NOTE Application Manager is executing, however its affinity is
            # not set to a particular CPU core
            try:
                # raise run time error exception
                raise RuntimeError
            except RuntimeError:
                # log the exception with traceback
                self.__logger.exception('Affinity could not be set.')

        # 2. fetch the application to be executed
        try:
            self.__application = self.__actions.get('action')
        except KeyError:
            # 'action' could not be found in 'actions' dictionary
            # log the exception with traceback
            self.__logger.exception("'action is not a valid key.")
            return Response.ERROR

        # 3. fetch the application parameters
        # application_args = self.__actions.get('args')
        # 4. fetch the action id
        try:
            self.__actions_id = self.__actions.get('action-id')
        except KeyError:
            # 'action-id' could not be found in 'actions' dictionary
            # log the exception with traceback
            self.__logger.exception("'action-id' is not a valid key.")
            return Response.ERROR

        # otherwise, action and action-id are found
        self.__logger.debug(
            f'application:{self.__application}, action_id:{self.__actions_id}')

        # 5. initialize the Communicator object for communication via Queues
        self.__communicator = CommunicatorQueue(
            self._log_settings, self._configurations_manager
        )
        # pre-processing is complete
        self.__logger.debug('pre-processing is done.')
        return Response.OK

    def run(self):
        """
        starts Application Manager for
        i)   executing the application,
        ii)  monitoring its resource usage, and
        iii) reading the outputs from the application.
        """
        # flag to indicate if something goes wrong
        # do post-processing such as set up its own affinity, get application
        # to be executed etc.
        if self.__pre_processing() == Response.ERROR:
            # Case a. something went wrong. Send ERROR as response to
            # Application Companion and terminate execution
            self.__send_response_to_application_companion(Response.ERROR)
            return Response.ERROR

        # 2. start fetching steering commands and execute them accordingly
        return self.__fetch_and_execute_steering_commands()
