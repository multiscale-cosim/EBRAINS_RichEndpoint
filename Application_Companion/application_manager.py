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
import re
import multiprocessing
import os
import subprocess
import time
import signal
import fcntl
import ast
import zmq

from common.utils import networking_utils

from EBRAINS_RichEndpoint.Application_Companion.signal_manager import SignalManager
from EBRAINS_RichEndpoint.Application_Companion.resource_usage_monitor import ResourceUsageMonitor
from EBRAINS_RichEndpoint.Application_Companion.common_enums import INTEGRATED_SIMULATOR_APPLICATION as SIMULATOR
from EBRAINS_RichEndpoint.Application_Companion.common_enums import MONITOR
from EBRAINS_RichEndpoint.Application_Companion.common_enums import Response
from EBRAINS_RichEndpoint.Application_Companion.common_enums import INTEGRATED_INTERSCALEHUB_APPLICATION as INTERSCALEHUB
from EBRAINS_RichEndpoint.Application_Companion.common_enums import SteeringCommands, COMMANDS
from EBRAINS_RichEndpoint.Application_Companion.db_manager_file import DBManagerFile
from EBRAINS_RichEndpoint.Application_Companion.affinity_manager import AffinityManager
from EBRAINS_RichEndpoint.Application_Companion.common_enums import SERVICE_COMPONENT_CATEGORY
from EBRAINS_RichEndpoint.Application_Companion.common_enums import SERVICE_COMPONENT_STATUS
from EBRAINS_RichEndpoint.registry_state_machine.state_enums import STATES
from EBRAINS_RichEndpoint.orchestrator.communicator_zmq import CommunicatorZMQ
from EBRAINS_RichEndpoint.orchestrator.zmq_sockets import ZMQSockets
from EBRAINS_RichEndpoint.orchestrator.communicator_queue import CommunicatorQueue
from EBRAINS_RichEndpoint.orchestrator.communication_endpoint import Endpoint
from EBRAINS_RichEndpoint.orchestrator.proxy_manager_client import ProxyManagerClient

from EBRAINS_ConfigManager.global_configurations_manager.xml_parsers.default_directories_enum import DefaultDirectories


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
                 # connection detials of Registry Proxy Manager
                 proxy_manager_connection_details,
                 # range of ports for Application Manager
                 port_range_for_application_manager,
                 # flag to enable/disable resource usage monitoring
                 enable_resource_usage_monitoring=True):
        multiprocessing.Process.__init__(self)
        self._log_settings = log_settings
        self._configurations_manager = configurations_manager
        self.__logger = self._configurations_manager.load_log_configurations(
            name=__name__,
            log_configurations=self._log_settings)

        # get client to Proxy Manager Server
        self._proxy_manager_client = ProxyManagerClient(
            self._log_settings,
            self._configurations_manager)

        # Connect with Proxy Manager Server
        # NOTE: it terminates with RuntimeError if connection could ne be made
        # for whatever reasons
        self._proxy_manager_client.connect(
            proxy_manager_connection_details["IP"],
            proxy_manager_connection_details["PORT"],
            proxy_manager_connection_details["KEY"],
        )

        # Now, get the proxy to registry manager
        self.__health_registry_manager_proxy =\
            self._proxy_manager_client.get_registry_proxy()

       
       # range of ports for Application Manager
        self.__port_range_for_application_manager = port_range_for_application_manager
        self.__application_manager_in_queue = None
        self.__application_manager_out_queue = None
        self.__rep_endpoint_with_application_companion = None
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
        self.__resource_usage_monitors = []
        self.__exit_status = None
        self.__endpoints_address = None
        self.__response_from_action = []
        self.__action_pids = []
        self.__am_registered_component_service = None

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
       # bind_with_cores = list(range(self.__bind_to_cpu[0] + 1,
        #                             self.__legitimate_cpu_cores))
        bind_with_cores = list(range(self.__bind_to_cpu[0]+1,
                                     8))
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
        strings = application
        application=[x.strip() for x in strings if x.strip()]  # TODO later do it in parser
        self.__logger.debug(f"action cmd after stripping white space:{application}")
        try:
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

        # NOTE disabling the functionality for hpc becuase it is set directly
        # in 'srun' command given to subprocess.Popen

        # if self.__set_affinity(self.__popen_process.pid) == Response.ERROR:
        #     # affinity could not be set, log exception with traceback
        #     # NOTE: application is launched with no defined affinity mask
        #     try:
        #         # raise runtime exception
        #         raise (RuntimeError)
        #     except RuntimeError:
        #         # log the exception with traceback details
        #         self.__logger.exception(
        #             self.__logger,
        #             f'affinity could not be set for '
        #             f'<{self.__actions_id}>:'
        #             f'{self.__popen_process.pid}')

        # Otherwise, everything goes right
        self.__logger.info(f'<{self.__actions_id}> starts execution')
        self.__logger.debug(f"PID:{os.getpid()} is executing the action "
                            f"<{self.__actions_id}>, "
                            f"PID={self.__popen_process.pid}")
        return Response.OK

    def __start_resource_usage_monitoring(self, pid):
        """
        helper function to start monitoring of the resources usage by the
        process having PID provided as parameter
        """
        # get affinity mask
        bind_with_cores = self.__affinity_manager.get_affinity(pid)
        # initialize resource usage monitor
        resource_usage_monitor = ResourceUsageMonitor(
            self._log_settings,
            self._configurations_manager,
            # self.__popen_process.pid,
            pid,
            bind_with_cores)
        # start monitoring
        # Case a, monitoring could not be started
        if resource_usage_monitor.start_monitoring() == Response.ERROR:
            return self.__terminate_with_error_loudly(
                    f'Could not start monitoring for <{self.__actions_id}>: '
                    f'{pid}')

        # Case b: monitoring starts
        self.__logger.debug("started monitoring the resource usage.")
        # keep track of running monitors
        running_monitor_to_pid =\
            {MONITOR.PID_PROCESS_BEING_MONITORED.name: pid,
                MONITOR.RESOURCE_USAGE_MONITOR.name: resource_usage_monitor}
        self.__resource_usage_monitors.append(running_monitor_to_pid)
        self.__logger.debug("currently running monitors: "
                            f"{self.__resource_usage_monitors}")
        return Response.OK

    def __stop_preemptory(self):
        """helper function to terminate the application forcefully."""
        self.__logger.critical("terminating preemptory")
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
                return self.__terminate_with_error_loudly(
                        f"could not terminate the process "
                        f"PID={self.__popen_process.pid}")

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

    def __convert_mpi_portname_to_dictionary(self, lines, first_key):
        """
        finds and extracts the local minimum step size information from
        std_out stream, and converts it to a dictionary.

        Parameters
        ----------
        lines : str
            output received from the application

        first_key: str
            substring to look for in output

        Returns
        -------
            int
                return code indicating whether the string is converted into
                dictionary
        """
        # NOTE as per protocol, the port_name is received as a response to
        # INIT command
        # it is received via (stdin) PIPE as a string in the following format
        # from an INTERSCALE_HUB:

        # {'PID': '<pid>',
        # 'DATA_EXCHANGE_DIRECTION': '<direction>',
        # 'MPI_CONNECTION_INFO': '<mpi port name>',
        # 'INTERCOMM_TYPE': '<intercomm_type>'}

        # STEP 1. find the starting index of response in the output received
        # from InterscaleHub

        # As per protocol the response starts with PID
        # look for all occurrences of PID in output received
        response = [i.start() for i in re.finditer(first_key, lines)]
        # find the index of closing curly bracket to separate the response
        # received from two or more MPI ranks
        ending_at = [i.start() for i in re.finditer('}', lines)]

        # STEP 2. covert response string to dictionary
        try:
            for running_index, starts_at in enumerate(response):
                # the index points to PID, the curly bracket {'PID'...
                # therefore starts at index-2
                interscalehub_endpoint = ast.literal_eval(
                    lines[starts_at - 2:ending_at[running_index]+1])
                self.__logger.debug(f"running dictionary: {interscalehub_endpoint}")
                self.__response_from_action.append(interscalehub_endpoint)
                self.__action_pids.append(interscalehub_endpoint.get("PID"))
            self.__logger.info(f"got responses: {self.__response_from_action}")
            return Response.OK
        except Exception:
            # Could not convert string into dict
            # log the exception with traceback and return with error
            self.__logger.exception('could not convert '
                                    f'{lines[running_index - 2:]} into '
                                    f'the dictionary.')
            return Response.ERROR

    def __convert_local_min_stepsize_to_dictionary(self, lines):
        """
        finds and extracts the local minimum step size information from
        std_out stream, and converts it to a dictionary.

        Parameters
        ----------
        lines : str
            output received from the application

        Returns
        -------
            int
                return code indicating whether the string is converted into
                dictionary
        """
        # NOTE as per protocol, the local minimum step size is received as a
        # response of INIT command from SIMULATORs
        # it is received via (stdin) PIPE as a string in the
        # following format from a Simulator:

        # {'PID': <pid>, 'LOCAL_MINIMUM_STEP_SIZE': <step_size>}

        # STEP 1. find the starting index of response in the output received
        # from Simulator

        # As per protocol the response starts with PID, so look for that in
        # output received
        index = lines.find(SIMULATOR.PID.name)

        # STEP 2. covert response string to dictionary
        try:
            # the index points to PID, the curly bracket {'PID'... therefore
            # starts at from index-2
            self.__logger.debug("string response before converting to a"
                                f"dictionary: {lines[index - 2:]}")
            self.__response_from_action = ast.literal_eval(lines[index - 2:])
            self.__action_pids.append(self.__response_from_action.get("PID"))
            self.__logger.info(f"got responses: {self.__response_from_action}")
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

                    # get local minimum step size received from the Simulator
                    # as a response to INIT command
                    if SIMULATOR.LOCAL_MINIMUM_STEP_SIZE.name in decoded_lines:
                        if self.__convert_local_min_stepsize_to_dictionary(
                                decoded_lines) == Response.ERROR:
                            # Case a. Local minimum step size could not be
                            # determined, terminate the execution with error
                            # NOTE an exception with traceback is already
                            # logged
                            self.__stop_preemptory()
                            return Response.ERROR

                        # Case b. All information from SIMULATORS (received as
                        # a response to INIT command) is read from PIPES, now
                        # execute other steering commands
                        break

                    # get MPI connection details received from the InterscaleHub
                    # as a response to INIT command
                    if INTERSCALEHUB.MPI_CONNECTION_INFO.name in decoded_lines:
                        if self.__convert_mpi_portname_to_dictionary(
                                decoded_lines,
                                INTERSCALEHUB.PID.name) == Response.ERROR:
                            # Case a. MPI connection details could not be
                            # determined, terminate the execution with error
                            # NOTE an exception with traceback is already
                            # logged
                            self.__stop_preemptory()
                            return Response.ERROR

                        # Case b. All Output from INTERSCALEHUB (as a response
                        # to INIT command) is read from PIPES, now execute
                        # other steering commands
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

        # everything goes right i.e. either application finished execution or
        # outputs are read as a response to the STEERING command being
        # executed currently
        return Response.OK

    def __conclude_resource_usage_monitoring(self):
        """
        1. Stops resource usage monitoring
        2. Dumps the monitoring data to a JSON file.
        """
        for resource_usage_monitor in self.__resource_usage_monitors:
            # get monitor
            monitor = resource_usage_monitor.get(
                MONITOR.RESOURCE_USAGE_MONITOR.name)
            monitored_process_pid = resource_usage_monitor.get(
                MONITOR.PID_PROCESS_BEING_MONITORED.name)
            # stop resource usage monitoring
            monitor.keep_monitoring = False
            # retrieve resource usage statistics
            resource_usage_summary =\
                monitor.get_resource_usage_stats(self.__exit_status)
            self.__logger.debug(f"Resource Usage stats: "
                                f"{resource_usage_summary.items()}")
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
                                        f'pid_{monitored_process_pid}'
                                        '_resource_usage_metrics.json')
            # dump the monitoring data
            self.__db_manager_file.write(
                metrics_file,
                resource_usage_summary)
        return Response.OK

    def __post_processing(self):
        """
        helper function to post process after application execution ends.

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
                return self.__terminate_with_error_loudly("post processing fails")

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
        sends response to Application Companion
        execution.

        Parameters
        ----------
        response : ...
            response to be sent to Application Companion

        Returns
        ------
            return code as int
        """
        self.__logger.debug(f"sending {response} to Application Companion.")
        # return self.__communicator.send(
        #     response, self.__application_manager_out_queue)
        return self.__communicator.send(
            response, self.__rep_endpoint_with_application_companion)

    def __execute_init_command(self, action):
        """
        Executes INIT steering command by launching the application.

        NOTE INIT of application (initialization of buffers, network,
        local minimum delay, etc.) is a system action and is done implicitly
        after launching.
        """
        # get actions to be executed
        self.__actions = action
        self.__logger.debug(f"actions: {self.__actions}")
        # 5. fetch the application to be executed
        try:
            self.__application = self.__actions.get('action')
            self.__logger.debug(f"application: {self.__application}")
        except KeyError:
            # 'action' could not be found in 'actions' dictionary
            # log the exception with traceback
            self.__logger.exception("'action is not a valid key.")
            return Response.ERROR
        
        # 1. update local state
        # NOTE The state is already transited to 'SYNCHRONIZING' with the
        # state transition of Aapplication Companion
        self.__logger.debug("state is already updated by Application Companion")

        # 2. get proxy to update the states later in registry
        self.__am_registered_component_service = (
            self.__health_registry_manager_proxy.find_by_id(os.getpid())
        )
        self.__logger.debug(
            "component service id: "
            f"{self.__am_registered_component_service.id};"
            f"name: {self.__am_registered_component_service.name}"
        )

        # 1. Launch application
        if self.__launch_application(self.__application) == Response.ERROR:
            # Case a, could not launch the application
            # send error as response to Application Companion
            self.__send_response_to_application_companion(Response.ERROR)
            # terminate with error
            return self.__terminate_with_error_loudly('Launching is failed. Quitting!')

        # Case b, application is launched successfully
        self.__logger.debug('application is launched.')

        # 3. Get response from application and send to Application Companion
        if self.__read_popen_pipes(self.__application) == Response.ERROR:
            # Case a, could not read the outputs
            # send error as response to Application Companion
            self.__send_response_to_application_companion(Response.ERROR)
            # terminate with error
            return self.__terminate_with_error_loudly('error reading output')

        # Case b, outputs are read successfully
        self.__logger.debug('outputs are read.')

        # 4. send local minimum step size as a response to Application
        # Companion
        self.__logger.debug('outputs are read from '
                            f'{self.__actions_id}: {self.__response_from_action}')
        self.__send_response_to_application_companion(
            self.__response_from_action)
        return Response.OK

    def __execute_start_command(self, global_minimum_step_size):
        """
        1. Executes START steering command by sending it to applications.
        2. Receives output from the application.
        3. Does post-processing after the execution of applications.
        """
        # 1. update local state
        self.__am_registered_component_service =\
            self.__update_local_state(SteeringCommands.START)
        if self.__am_registered_component_service == Response.ERROR:
            # terminate loudly as state could not be updated
            # exception is already logged with traceback
            return self.__respond_with_state_update_error()
        
        # 2. start resource usage monitoring, if enabled
        if self.__is_monitoring_enabled:
            self.__logger.info(f"starting monitoring for PIDs: {self.__action_pids}")
            for action_pid in self.__action_pids:
                if self.__start_resource_usage_monitoring(action_pid) == Response.ERROR:
                    # monitoring could not be started, a relevant exception is
                    # already logged with traceback
                    return Response.ERROR

        # 3. send command to application
        self.__send_command_to_application(SteeringCommands.START.name)

        # 4. Read outputs from the application
        if self.__read_popen_pipes(self.__application) == Response.ERROR:
            # Case a, could not read the outputs
             # send error as response to Application Companion
            self.__send_response_to_application_companion(Response.ERROR)
            # terminate with error
            return self.__terminate_with_error_loudly('error reading output')

        # Case b, outputs are read successfully
        self.__logger.debug('outputs are read.')

        # 5. process is finished, do post-processing (e.g. stop monitoring etc)
        if self.__post_processing() == Response.ERROR:
            # send error as response to Application Companion
            self.__send_response_to_application_companion(Response.ERROR)
            # an exception is already logged with trace back in callee
            # function, now terminate with error
            return Response.ERROR

        # Case b, post-processing is done
        self.__logger.info('post processing is done.')

        # send response to Application Companion
        self.__send_response_to_application_companion(Response.OK)
        return Response.OK

    def __execute_end_command(self, parameters):
        """
        Checks the exit status of the application.

        NOTE later add other functionality here such as post data analysis etc.
        """
        # 1. update local state
        self.__am_registered_component_service =\
            self.__update_local_state(SteeringCommands.END)
        if self.__am_registered_component_service == Response.ERROR:
            # terminate loudly as state could not be updated
            # exception is already logged with traceback
            return self.__respond_with_state_update_error()
        
        # 2. Check whether the application executed successfully
        self.__exit_status = self.__popen_process.poll()
        #  Case a, something went wrong during application execution
        if not self.__exit_status == 0:
            # send error as response to Application Companion
            self.__send_response_to_application_companion(Response.ERROR)
            # terminate with error
            return self.__terminate_with_error_loudly(
                f"application <{self.__application}> "
                f" action: <{self.__actions_id}> execution "
                f"went wrong, finished with rc = {self.__exit_status}"
            )

        # Case b, application executed successfully
        self.__logger.info(f'Action <{self.__actions_id}> finished properly.')
        self.__send_response_to_application_companion(Response.OK)
        return Response.OK

    def __respond_with_state_update_error(self):
        """
        i)  informs Applicaiton Compnaion about local state update failure.
        ii) logs the exception with traceback and terminates loudly with error.
        """
        # inform Applocation Companion about state update failure
        self.__send_response_to_application_companion(EVENT.STATE_UPDATE_FATAL)
        # log the exception with traceback details
        return self.__terminate_with_error_loudly("Could not update state. Quitting!")
    
    def __setup_communicators(self):
        """helper function to set up communicators"""
        # initialize the Communicator object for communication via Queues with
        # Application Manager
        if self.__port_range_for_application_manager:
            # initialize the Communicator object for communication via ZMQ with
            # Command&Control service
            self.__communicator = CommunicatorZMQ(
                self._log_settings,
                self._configurations_manager)
            self.__logger.debug(f"communicator is set: {self.__communicator}")

        else:
            self.__communicator = CommunicatorQueue(
            self._log_settings,
            self._configurations_manager)
            self.__logger.debug(f"communicator is set: {self.__communicator}")
    
    def __terminate_with_error_loudly(self, custom_message):
        """        
        raises RuntimeException with custom message and return Error as a
        response to terminate with Error.
        """
        try:
            # raise runtime exception
            raise RuntimeError
        except RuntimeError:
            # log the exception with traceback details
            self.__logger.exception(custom_message)
            # terminate with ERROR
            return Response.ERROR
    
    def __parse_command(self):
        """
        helper function to parse commands received from Applicaiton Companion
        """
        command = self.__communicator.receive(
                self.__rep_endpoint_with_application_companion)
        self.__logger.debug(f"command received: {command}")
        try:
            current_steering_command = command.get(COMMANDS.STEERING_COMMAND.name)
            parameters = command.get(COMMANDS.PARAMETERS.name)
        except ValueError:
            self.__terminate_with_error_loudly("error in parsing command")
        
        return current_steering_command, parameters
    
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
            current_steering_command, parameters = self.__parse_command()
            self.__logger.debug(f"got the command: {current_steering_command}")
            self.__logger.debug(f"got the parameters: {parameters}")
            # 2. execute the current steering command
            if command_execution_choices[current_steering_command](parameters) ==\
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
    
    def __setup_endpoints(self):
        # if the range of ports are not provided then use the shared queues
        # assuming that it is to be deployed on laptop/single node
        if self.__port_range_for_application_manager is None:
            # proxies to the shared queues
            # for in-coming messages
            self.__application_manager_in_queue = multiprocessing.Manager().Queue()
            # for out-going messages
            self.__application_manager_out_queue = multiprocessing.Manager().Queue()
            self.__endpoints_address = (self.__application_manager_in_queue,
                                        self.__application_manager_out_queue)
            return Response.OK

        else:   # create ZMQ endpoints
            self.__zmq_sockets = ZMQSockets(self._log_settings, self._configurations_manager)
            # Endpoint with Application Companion via a REP socket
            self.__rep_endpoint_with_application_companion =\
                self.__zmq_sockets.create_socket(zmq.REP)
            self.__my_ip = networking_utils.my_ip()  # get IP address
            # get the port bound to REP socket to communicate with Application
            # Companion
            port_with_application_companion =\
                self.__zmq_sockets.bind_to_first_available_port(
                    zmq_socket=self.__rep_endpoint_with_application_companion,
                    ip=self.__my_ip,
                    min_port=self.__port_range_for_application_manager['MIN'],
                    max_port=self.__port_range_for_application_manager['MAX'],
                    max_tries=self.__port_range_for_application_manager['MAX_TRIES']
                )
            
            # check if port could not be bound
            if port_with_application_companion is Response.ERROR:
                # NOTE the relevant exception with traceback is already logged
                # by callee function
                self.__logger.error("port with Application Companion could not"
                                    " be bound")
                # return with error to terminate
                return Response.ERROR

            # everything went well, now create endpoints address
            endpoint_address_with_application_companion =\
                Endpoint(self.__my_ip, port_with_application_companion)
            
            self.__endpoints_address = {
                SERVICE_COMPONENT_CATEGORY.APPLICATION_COMPANION: endpoint_address_with_application_companion}

            return Response.OK

    def __update_local_state(self, input_command):
        """
        updates the local state.

        Parameters
        ----------

        input_command: SteeringCommands.Enum
            the command to transit from the current state to next legal state

        Returns
        ------
            return code as int
        """
        return self.__health_registry_manager_proxy.update_local_state(
            self.__am_registered_component_service, input_command
        )
    
    def __pre_processing(self):
        """
        Does pre-processing such as set affinity for its won self, set up
        Communicator, get application to be executed, etc.
        """
        self.__logger.info('setting up Application Manager')
        pid = os.getpid()  # get its own PID

        # 1. bind itself with a user defined single CPU core e.g. CPU core 1
        if self.__affinity_manager.set_affinity(
                pid, self.__bind_to_cpu) == Response.ERROR:
            # Case a, affinity is not set
            # NOTE Application Manager is executing, however its affinity is
            # not set to a particular CPU core
            self.__terminate_with_error_loudly('Affinity could not be set.')
        
        # 2. fetch the application parameters
        # fetch the action id
        try:
            self.__actions_id = self.__actions.get('action-id')
        except KeyError:
            # 'action-id' could not be found in 'actions' dictionary
            # log the exception with traceback
            self.__logger.exception("'action-id' is not a valid key.")
            return Response.ERROR

        # otherwise, action-id is found
        self.__logger.debug(f'action_id:{self.__actions_id}')

        # 3. setup endpoints for communication
        self.__setup_endpoints()
        
        # 4. register with Registry
        # prepare name for proxy
        # TODO set proxy_name to action_type
        proxy_name = self.__actions_id+"_"+"Application_Manager"
        self.__logger.debug(f"proxy_name: {proxy_name}")
        # register the proxy
        if (
            self.__health_registry_manager_proxy.register(
                os.getpid(),  # id
                proxy_name,  # name
                SERVICE_COMPONENT_CATEGORY.APPLICATION_MANAGER,  # category
                self.__endpoints_address,  # endpoint
                SERVICE_COMPONENT_STATUS.UP,  # current status
                # current state
                STATES.READY
            ) == Response.ERROR
        ):
            # Case, registration fails
            self.__terminate_with_error_loudly("Could not be registered. Quitting!")

        # Otherwise, indicate a successful registration
        self.__logger.info("registered with registry.")

        # 5. initialize the Communicator object for communication
        # self.__communicator = CommunicatorQueue(
        #     self._log_settings, self._configurations_manager
        # )
        self.__setup_communicators()
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
