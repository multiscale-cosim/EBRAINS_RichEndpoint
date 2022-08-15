# ------------------------------------------------------------------------------
#  Copyright 2020 Forschungszentrum Jülich GmbH and Aix-Marseille Université
# "Licensed to the Apache Software Foundation (ASF) under one or more contributor
#  license agreements; and to You under the Apache License, Version 2.0. "
#
# Forschungszentrum Jülich
#  Institute: Institute for Advanced Simulation (IAS)
#    Section: Jülich Supercomputing Centre (JSC)
#   Division: High Performance Computing in Neuroscience
# Laboratory: Simulation Laboratory Neuroscience
#       Team: Multi-scale Simulation and Design
# ------------------------------------------------------------------------------
import multiprocessing
import os
import signal

from common.utils import proxy_manager_server_utils
from EBRAINS_RichEndpoint.Application_Companion.signal_manager import SignalManager
from EBRAINS_RichEndpoint.Application_Companion.common_enums import EVENT, INTEGRATED_SIMULATOR_APPLICATION
from EBRAINS_RichEndpoint.Application_Companion.common_enums import SteeringCommands
from EBRAINS_RichEndpoint.Application_Companion.common_enums import Response
from EBRAINS_RichEndpoint.Application_Companion.common_enums import SERVICE_COMPONENT_CATEGORY
from EBRAINS_RichEndpoint.Application_Companion.common_enums import SERVICE_COMPONENT_STATUS
from EBRAINS_RichEndpoint.orchestrator.communicator_queue import CommunicatorQueue
from EBRAINS_RichEndpoint.orchestrator.proxy_manager_client import ProxyManagerClient
from EBRAINS_RichEndpoint.orchestrator.health_status_monitor import HealthStatusMonitor
from EBRAINS_RichEndpoint.registry_state_machine.state_enums import STATES


class Orchestrator(multiprocessing.Process):
    def __init__(self, log_settings, configurations_manager):
        multiprocessing.Process.__init__(self)
        self._log_settings = log_settings
        self._configurations_manager = configurations_manager
        self.__logger = self._configurations_manager.load_log_configurations(
                                        name=__name__,
                                        log_configurations=self._log_settings)
        # settings for signal handling
        self.__signal_manager = SignalManager(self._log_settings,
                                              self._configurations_manager)
        signal.signal(signal.SIGINT,
                      self.__signal_manager.interrupt_signal_handler
                      )
        signal.signal(signal.SIGTERM,
                      self.__signal_manager.kill_signal_handler
                      )

        # proxies to the shared queues
        self.__orchestrator_in_queue =\
            multiprocessing.Manager().Queue()  # for in-comming messages
        self.__orchestrator_out_queue =\
            multiprocessing.Manager().Queue()  # for out-going messages

        # get client to Proxy Manager Server
        self._proxy_manager_client = ProxyManagerClient(
            self._log_settings,
            self._configurations_manager)

        # Connect with Proxy Manager Server
        # NOTE: it terminates with RuntimeError if connection could ne be made
        # for whatever reasons
        self._proxy_manager_client.connect(
            proxy_manager_server_utils.IP,
            proxy_manager_server_utils.PORT,
            proxy_manager_server_utils.KEY,
        )

        # Now, get the proxy to registry manager
        self.__health_registry_manager_proxy =\
            self._proxy_manager_client.get_registry_proxy()

        # flag to indicate whether Orchestrator is registered with registry
        self.__is_registered = multiprocessing.Event()

        # instantiate the global health and status monitor
        self.__global_health_monitor = HealthStatusMonitor(
                                        self._log_settings,
                                        self._configurations_manager,
                                        self.__health_registry_manager_proxy)

        self.__step_sizes = None
        self.__steering_commands_history = []
        self.__responses_received = []
        self.__command_and_control_service = []
        self.__command_and_steering_service_in_queue = None
        self.__command_and_steering_service_out_queue = None
        self.__orchestrator_registered_component = None
        self.__communicator = None
        self.__logger.debug("Orchestrator is initialized.")

    @property
    def is_registered_in_registry(self): return self.__is_registered

    @property
    def steering_commands_history(self): return self.__steering_commands_history

    def __start_global_health_monitoring(self):
        """start global health monitoring thread."""
        self.__global_health_monitor.start_monitoring()

    def __finalize_global_health_monitoring(self):
        """concludes global health monitoring."""
        self.__logger.info("concluding the health and status monitoring.")
        self.__global_health_monitor.finalize_monitoring()
    
    def __get_component_from_registry(self, target_components_category) -> list:
        """
        helper function for retreiving the proxy of registered components by
        category.

        Parameters
        ----------
        target_components_category : SERVICE_COMPONENT_CATEGORY.Enum
            Category of target service components

        Returns
        ------
        components: list
            list of components matches category with parameter
            target_components_category
        """
        components = self.__health_registry_manager_proxy.\
            find_all_by_category(target_components_category)
        self.__logger.debug(
            f'found components: {len(components)}')
        return components

    def __update_local_state(self, input_command):
        """
        helper function for updating the local state.

        Parameters
        ----------
        
        input_command: SteeringCommands.Enum
            the command to transit from the current state to next legal state

        Returns
        ------
        response code: int
            response code indicating whether or not the state is updated.
        """
        return self.__health_registry_manager_proxy.update_local_state(
                                    self.__orchestrator_registered_component,
                                    input_command)
        

    def __find_global_minimum_step_size(self, step_sizes_with_pids):
        """
        helper function for finding the minimum step size.

         Parameters
        ----------
        step_sizes_with_pids : list
            list of dictionaries containing the PIDs and step sizes.

        Returns
        ------
        minimum step size: float
            the minimum step size of the list.
        """
        # extract all step sizes from dictionary
        for dic in step_sizes_with_pids:
            if dic == {}:
                step_sizes_with_pids.remove(dic)
        step_sizes = [sub[INTEGRATED_SIMULATOR_APPLICATION.LOCAL_MINIMUM_STEP_SIZE.name] for sub in step_sizes_with_pids]
        self.__logger.debug(f'step_sizes: {step_sizes}')
        return min(step_sizes)

    def __receive_responses(self):
        '''
        helper function for receiving the responses from Application Companions.
        '''
        try:
            return self.__communicator.receive(
                    self.__command_and_steering_service_out_queue)
        except Exception:
            # Log the exception with Traceback details
            self.__logger.exception('exception while getting response.')
            return Response.ERROR

    def __process_responses(self, responses, steering_command):
        '''
        helper function to process the received responses.

        Parameters
        ----------
        responses : Any
            responses received from Application Companions.

       steering_command: SteeringCommands.Enum
            Steering Command that was sent to Application Companion.

        Returns
        ------
        returns the processed response.
        '''
        self.__logger.debug(f'got the response: {responses}')
        # Case, received local state update failure as response
        if EVENT.STATE_UPDATE_FATAL in responses:
            self.__logger.critical('directing C&C to terminate with error.')
            # send terminate command to C&C service
            self.__send_terminate_command(EVENT.STATE_UPDATE_FATAL)
            # stop monitoring
            self.__logger.critical('finalizing monitoring.')
            self.__finalize_global_health_monitoring()
            # terminate processing with error
            return Response.ERROR

        # Case, find the minimum step-size if steering command is INIT
        if steering_command == SteeringCommands.INIT:
            self.__step_sizes = responses
            self.__logger.debug(f'step_sizes and PIDs: {self.__step_sizes}')
            min_step_size = self.__find_global_minimum_step_size(self.__step_sizes)
            self.__logger.info(f'Global Minimum Step Size: {min_step_size}')
            return min_step_size

        # Case, response is e.g. RESPONSE.OK, etc.
        # keep track of responses received
        return self.__responses_received.append(responses)

    def __send_terminate_command(self, fatal_event):
        '''
        helper function to send termination command to other components
        in case if something went wrong fatally such as local state update
        failure, etc.
        '''
        # send terminate command to C&C service
        self.__communicator.send(
                        fatal_event,
                        self.__command_and_steering_service_in_queue)

    def __execute_steering_command(self, steering_command):
        """
        helper function for executing the Steering Commands.
        """
        self.__logger.debug(f'Executing steering command: {steering_command}!')
        # 1. send steering command to C&C service
        # Case a, something went wrong while sending
        if self.__communicator.send(
                steering_command,
                self.__command_and_steering_service_in_queue) ==\
                Response.ERROR:
            try:
                # sending failed, raise an exception
                raise RuntimeError
            # NOTE relevant exception is already logged by Communicator
            except Exception:
                # log the exception with traceback
                self.__logger.exception('could not send the command.')
            # return with with error
            return Response.ERROR

        # Case b, command is sent, record the command in history
        self.__steering_commands_history.append(steering_command.name)
        # 2. Receive the responses
        self.__logger.debug('getting the response.')
        responses = self.__receive_responses()
        # Case a, something went wrong while getting response
        if responses == Response.ERROR:
            # NOTE exception with traceback is already logged
            # return with error
            return Response.ERROR

        # Case b, responses received successfully
        # 3. process responses
        if self.__process_responses(responses,
                                    steering_command) == Response.ERROR:
            # return with error
            return Response.ERROR

        # Case, the command is executed successfully
        self.__logger.debug(f'Successfully executed the command:'
                            f'{steering_command.name}')
        self.__logger.info(f'Current global state: {self.current_global_state()}')
        self.__logger.info(f'uptime till now: {self.up_time_till_now()}')
        return Response.OK

    def __execute_if_validated(self, steering_command, valid_state):
        '''
        Executes the steering command if the global state is valid.
        Updates the global state after successful execution of the command.

        Parameters
        ----------
        steering_command : SteeringCommands.Enum
            Steering Command to execute

        valid_state: STATES.Enum
            valid global state to execute the Steering Command

        new_state: STATES.Enum
            new global state to be transitioned after successful execution of
            the Steering Command

        Returns
        ------
        response code: int
            response code indicating if the command is successfully executed
            and the global state is transitioned
        '''
        # i. check if the global state is valid for steering_command execution
        if self.current_global_state() != valid_state:
            self.__logger.critical(
                f'Global state must be {valid_state} for executing'
                f'the steering command: {steering_command}')
            return Response.ERROR

        # ii. update local state
        state_before_local_state = self.__orchestrator_registered_component.current_state
        self.__orchestrator_registered_component = self.__update_local_state(steering_command)
        if self.__orchestrator_registered_component == Response.ERROR:
            # local state could not be updated
            # NOTE an exception with traceback has already been logged in
            # callee function
            self.__logger.critical('Error updating the local state.')
            return Response.ERROR
        state_after_local_state = self.__orchestrator_registered_component.current_state
        self.__health_registry_manager_proxy.update_state_transition_history(
            state_before_local_state.name, steering_command.name, state_after_local_state.name
        )

        # iii. send steering command to Application Companions
        self.__logger.debug(f'sending the command: {steering_command.name}')
        if self.__execute_steering_command(steering_command) == Response.ERROR:
            self.__logger.critical(f'Error executing steering command: '
                                   f'{steering_command}')
            return Response.ERROR

        # iv. update global state if it is not yet updated by global health monitor
        if self.__update_global_state() == Response.ERROR:
            self.__logger.critical('Error updating the global state.')
            return Response.ERROR

        # everything goes right
        return Response.OK

    def __register_with_registry(self):
        '''helper function to register with registry.'''
        if self.__health_registry_manager_proxy.register(
                        os.getpid(),  # id
                        SERVICE_COMPONENT_CATEGORY.ORCHESTRATOR,   # category
                        SERVICE_COMPONENT_CATEGORY.ORCHESTRATOR,   # name
                        (self.__orchestrator_in_queue,  # endpoint
                         self.__orchestrator_out_queue),
                        SERVICE_COMPONENT_STATUS.UP,  # current status
                        STATES.READY) == Response.ERROR:  # current state
            # Case, registration fails
            try:
                # raise run time error exception
                raise RuntimeError
            except RuntimeError:
                # log the exception with traceback
                self.__logger.exception('Could not be registered. Quiting!')
                # raise signal to terminate
                signal.raise_signal(signal.SIGTERM)
            # terminate with error
            return Response.ERROR

        # Case, registration is done
        # indicate a successful registration
        self.__is_registered.set()
        # retrieve registered component which is later needed to update states
        self.__orchestrator_registered_component =\
            self.__health_registry_manager_proxy.find_by_id(os.getpid())
        self.__logger.debug(
            f'component service id: '
            f'{self.__orchestrator_registered_component.id}'
            f'; name: {self.__orchestrator_registered_component.name}')
        return Response.OK

    def __set_up_runtime(self):
        """
        helper function for setting up runtime such as register with registry,
        update global state, etc. before starting orchestration.
        """
        # register with registry
        if self.__register_with_registry() == Response.ERROR:
            # terminate with ERROR
            return Response.ERROR
        # fetch C&C from registry
        self.__command_and_control_service =\
            self.__get_component_from_registry(
                        SERVICE_COMPONENT_CATEGORY.COMMAND_AND_CONTROL)
        self.__logger.debug(f'command and steering service: '
                            f'{self.__command_and_control_service[0]}')
        # fetch C&C endpoint (in_queue and out_queue)
        self.__command_and_steering_service_in_queue,\
            self.__command_and_steering_service_out_queue =\
            self.__command_and_control_service[0].endpoint
        # initialize the Communicator object for communication via Queues
        self.__communicator = CommunicatorQueue(self._log_settings,
                                                self._configurations_manager)
        # update global state to 'READY' assuming all components are already
        # up and running with local state 'READY'. The assumption is anyway
        # validated during the update process.
        if self.__update_global_state() == Response.ERROR:
            self.__logger.critical('Error updating the global state.')
            return Response.ERROR

        # start monitoring threads
        self.__start_global_health_monitoring()
        return Response.OK

    def __terminate_with_error(self):
        '''
        helper function to terminate with error and so to command other
        Components such as Command and Control, etc.
        '''
        self.__logger.critical('terminating with error. ')
        # send command to Application Companions and C&C service to terminate
        # with error
        self.__send_terminate_command(EVENT.FATAL)
        # stop monitoring
        self.__global_health_monitor.finalize_monitoring()
        # terminate with error
        return Response.ERROR

    def __execute_init_command(self):
        # validate the local and global states and execute
        return self.__execute_if_validated(SteeringCommands.INIT,
                                           STATES.READY)

    def __execute_start_command(self):
        # validate the local and global states and execute
        return self.__execute_if_validated(SteeringCommands.START,
                                           STATES.SYNCHRONIZING)

    def __execute_end_command(self):
        # validate the local and global states and execute
        return self.__execute_if_validated(SteeringCommands.END, STATES.RUNNING)

    def __handle_fatal_event(self):
        self.__logger.critical('quitting forcefully!')
        # return with ERROR to indicate preemptory exit
        # NOTE an exception is logged with traceback by calling function
        # when responded with 'ERROR'
        return Response.ERROR

    def __command_control_and_coordinate(self):
        '''
        Main loop to command, control and coordinate the other components.
        The loop terminates either by normally or forcefully such that
        i)  Normally: receiving the steering command END, or by
        ii) Forcefully: receiving the FATAL command i.e. either due to pressing
        CTRL+C, or if the global state is ERROR.
        '''
        # create a dictionary of choices for the steering commands and
        # their corresponding executions
        command_execution_choices = {
                        EVENT.FATAL: self.__handle_fatal_event,
                        SteeringCommands.INIT: self.__execute_init_command,
                        SteeringCommands.START: self.__execute_start_command,
                        SteeringCommands.END: self.__execute_end_command}
        while True:
            self.__logger.debug(
                    f'current global state: '
                    f'{self.__health_registry_manager_proxy.current_global_state()}')
            # fetch the steering command
            current_steering_command = self.__communicator.receive(
                                                self.__orchestrator_in_queue)
            self.__logger.debug(f'got the command {current_steering_command}')
            # execute the steering command
            if command_execution_choices[current_steering_command]() ==\
                    Response.ERROR:
                # something went wrong
                try:
                    # raise run time error exception
                    raise RuntimeError
                except RuntimeError:
                    # log the exception with traceback
                    self.__logger.exception(
                        f'error executing: {current_steering_command.name}')
                # terminate loudly with error
                self.__orchestrator_out_queue.put(self.__terminate_with_error())
                return Response.ERROR

            # finish execution as normal after executing END command
            if current_steering_command == SteeringCommands.END:
                # log the steering commands sent to Application Companions
                self.__logger.info('Steering commands history: '
                                   f'{self.__steering_commands_history}')
                # log the local state transition traceback
                local_state_transition_history =\
                    self.__health_registry_manager_proxy.local_state_transition_history()
                self.__logger.info('Local state transition history: '
                                   f'{local_state_transition_history}')
                # log the global state_transition traceback
                global_state_transition_history =\
                    self.__health_registry_manager_proxy.global_state_transition_history()
                self.__logger.info('Global state transition history: '
                                   f'{global_state_transition_history}')
                # finish execution as normal
                self.__logger.info('Concluding orchestration.')
                self.__orchestrator_out_queue.put(Response.OK)
                return Response.OK

            # Execution is not yet ended, fetch the next steering commands
            self.__orchestrator_out_queue.put(Response.OK)
            continue

    def current_global_state(self):
        """Wrapper to get the current global state of the System."""
        return self.__health_registry_manager_proxy.current_global_state()

    def current_global_status(self):
        """Wrapper to get the current global status of the System."""
        return self.__health_registry_manager_proxy.current_global_status()

    def __update_global_state(self):
        """Wrapper to update the current global state of the System."""
        return self.__health_registry_manager_proxy.update_global_state()

    def up_time_till_now(self):
        """Wrapper to get the up time of the system since the start."""
        return self.__health_registry_manager_proxy.system_up_time()
    
    def run(self):
        """
        executes the steering and commands, and orchestrates the workflow.
        """
        # Setup runtime such as register with registry etc.
        if self.__set_up_runtime() == Response.ERROR:
            # NOTE exceptions are already logged at source of failure
            self.__logger.error('Setting up runtime failed, Quitting!.')
            # terminate with error
            self.__orchestrator_out_queue.put(Response.ERROR)
            return Response.ERROR

        # Runtime setup is done, start orchestration
        # self.__orchestrator_out_queue.put(self.__command_control_and_coordinate())
        return self.__command_control_and_coordinate()
        # return 0
