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
import sys
import signal
import zmq 
import pickle
import base64

from EBRAINS_Launcher.common.utils import networking_utils
from EBRAINS_Launcher.common.utils.security_utils import check_integrity
from EBRAINS_Launcher.common.utils import multiprocess_utils

from EBRAINS_RichEndpoint.orchestrator.communicator_queue import CommunicatorQueue
from EBRAINS_RichEndpoint.orchestrator.communicator_zmq import CommunicatorZMQ
from EBRAINS_RichEndpoint.orchestrator.proxy_manager_client import ProxyManagerClient
from EBRAINS_RichEndpoint.orchestrator.health_status_monitor import HealthStatusMonitor
from EBRAINS_RichEndpoint.orchestrator.zmq_sockets import ZMQSockets
from EBRAINS_RichEndpoint.orchestrator.communication_endpoint import Endpoint
from EBRAINS_RichEndpoint.orchestrator.control_command import ControlCommand
from EBRAINS_RichEndpoint.application_companion.signal_manager import SignalManager
from EBRAINS_RichEndpoint.application_companion.common_enums import EVENT, INTEGRATED_SIMULATOR_APPLICATION
from EBRAINS_RichEndpoint.application_companion.common_enums import SteeringCommands
from EBRAINS_RichEndpoint.application_companion.common_enums import Response
from EBRAINS_RichEndpoint.application_companion.common_enums import SERVICE_COMPONENT_CATEGORY
from EBRAINS_RichEndpoint.application_companion.common_enums import SERVICE_COMPONENT_STATUS
from EBRAINS_RichEndpoint.registry_state_machine.state_enums import STATES

from EBRAINS_ConfigManager.global_configurations_manager.xml_parsers.configurations_manager import ConfigurationsManager


class Orchestrator:
    def __init__(self, log_settings, configurations_manager,
                 proxy_manager_connection_details,
                 port_range=None):
        self._log_settings = log_settings
        self._configurations_manager = configurations_manager
        self.__logger = self._configurations_manager.load_log_configurations(
                                        name="Orchestrator",
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
       
        # get client to Proxy Manager Server
        self._proxy_manager_client = ProxyManagerClient(
            self._log_settings,
            self._configurations_manager)

        # Connect with Proxy Manager Server
        # NOTE: it terminates with RuntimeError if connection could not be made
        # for whatever reasons
        self._proxy_manager_client.connect(
            proxy_manager_connection_details["IP"],
            proxy_manager_connection_details["PORT"],
            proxy_manager_connection_details["KEY"],
        )

        # Now, get the proxy to registry manager
        self.__health_registry_manager_proxy =\
            self._proxy_manager_client.get_registry_proxy()

        # instantiate the global health and status monitor
        self.__global_health_monitor = HealthStatusMonitor(
                                        self._log_settings,
                                        self._configurations_manager,
                                        self.__health_registry_manager_proxy)

        self.__step_sizes = None
        self.__global_min_step_size = None
        self.__spike_detectors = None
        self.__steering_commands_history = []
        self.__responses_received = []
        self.__command_and_control_service = []
        self.__command_and_steering_service_endpoint = None
        self.__orchestrator_registered_component = None
        self.__port_range = port_range
        self.__communicator = None
        self.__control_command = None
        # self.__communicator_zmq = None
        self.__logger.debug("Orchestrator is initialized.")

    @property
    def steering_commands_history(self): return self.__steering_commands_history

    @property
    def global_minimum_step_size(self): return self.__global_min_step_size

    def __start_global_health_monitoring(self):
        """start global health monitoring thread."""
        self.__global_health_monitor.start_monitoring()

    def __finalize_global_health_monitoring(self):
        """concludes global health monitoring."""
        self.__logger.info("concluding the health and status monitoring.")
        self.__global_health_monitor.finalize_monitoring()
    
    def __get_component_from_registry(self, target_components_category) -> list:
        """
        helper function for retrieving the proxy of registered components by
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

    def __remove_empty_responses(self, responses_from_actions):
        # remove empty responses e.g. '{}' i.e. responses without step_sizes from InterscaleHubs
        responses = []
        for response in responses_from_actions:
            self.__logger.debug(f"running dictionary of response: {response}")
            if response == {}:
                continue
            else:
                # append running dictionary containing pid and stepsize to list
                responses.append(response)

        self.__logger.debug(f"after removing empty responses: {responses}")
        return responses

    def __spike_detectors_ids(self, responses_from_actions):
        """
        helper function for finding the id of first spike detectors

         Parameters
        ----------
        step_sizes_with_pids : list
            list of dictionaries containing the PIDs and step sizes.

        Returns
        ------
        minimum step size: float
            the minimum step size of the list.
        """
        spike_detectors = None
        # extract spike detectors from responses
        for response in responses_from_actions:
            spike_detectors = response.get('SPIKE_DETECTORS')
            if spike_detectors:
                self.__logger.debug(f'spike_detectors: {spike_detectors}')
                # return spike detectots
                return spike_detectors

        if not spike_detectors:
            # the response does not contain spike detectors
            # log the warning to notify
            self.__logger.critical("could not find spike_detectors in: "
                                    f"{responses_from_actions}")
            return spike_detectors

    def __find_global_minimum_step_size(self, responses_from_actions):
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
        # find minimum step size in the list of responses
        try:
            # extract all step sizes from the list
            self.__step_sizes = [
                sub[INTEGRATED_SIMULATOR_APPLICATION.LOCAL_MINIMUM_STEP_SIZE.name]
                for sub in responses_from_actions]
            
            self.__logger.debug(f'received step_sizes: {self.__step_sizes}')
            # return minimum step size in the list
            return min(self.__step_sizes)
        except KeyError:
            # the response does not contain step_size
            # log exception with traceback
            self.__logger.exception("could not find step-size in: "
                                    f"{responses_from_actions}")
            return Response.ERROR

    def __receive_responses(self):
        '''
        helper function for receiving the responses from Application Companions
        via C&C channel.
        '''
        self.__logger.debug('getting the response.')
        return self.__communicator.receive(
            self.__endpoint_with_command_control_service)

    # def __receive_responses(self):
    #     '''
    #     helper function for receiving the responses from Application Companions.
    #     '''
    #     try:
    #         return self.__communicator.receive(
    #                 self.__command_and_steering_service_out_queue)
    #     except Exception:
    #         # Log the exception with Traceback details
    #         self.__logger.exception('exception while getting response.')
    #         return Response.ERROR

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
        if EVENT.STATE_UPDATE_FATAL in responses\
                or EVENT.FATAL in responses\
                or Response.ERROR in responses:
            self.__logger.critical('directing C&C to terminate with error.')
            # send terminate command to C&C service
            self.__send_terminate_command(EVENT.STATE_UPDATE_FATAL)
            # stop monitoring
            self.__logger.critical('finalizing monitoring.')
            self.__finalize_global_health_monitoring()
            # terminate processing with error
            return Response.ERROR

        # remove empty responses
        valid_responses = self.__remove_empty_responses(responses)
        self.__logger.debug(f'valid_responses: {valid_responses}')
        # Case, find the minimum step-size if steering command is INIT
        if steering_command == SteeringCommands.INIT:
            self.__global_min_step_size = self.__find_global_minimum_step_size(
                valid_responses)
            # check if global minimum step size could not be determined
            if self.__global_min_step_size == Response.ERROR:
                # terminate with Error
                return Response.ERROR

            # Otherwise, global minimum step size has been determined
            # successfully
            self.__logger.info('Global Minimum Step Size: '
                               f'{self.__global_min_step_size}')

            # NEST shares spike detectors ids for some usecases in reponse
            # to INIT command by protocol
            # get the ids from response
            self.__spike_detectors = self.__spike_detectors_ids(valid_responses)
            if not self.__spike_detectors:
                # NOTE for some usecases its not required to expose spike detctors
                # Best to log it and move forward
                self.__logger.debug("spike detectors ids are not shared")
            else:
                # Otherwise, spike detectors ids are extracted successfully
                self.__logger.info(f'Spike Detectors ids: {self.__spike_detectors}')

        # keep track of received responses
        # NOTE By protocol, response is not minimum step_size if the steering
        # command is not INIT, instead it could be e.g. RESPONSE.OK, etc.
        self.__responses_received.append(responses)
        return Response.OK

    def __send_terminate_command(self, fatal_event):
        '''
        helper function to send termination command to other components
        in case if something went wrong fatally such as local state update
        failure, etc.
        '''
        # send terminate command to C&C service
        self.__communicator.send(
                        fatal_event,
                        self.__endpoint_with_command_control_service)

    def __execute_steering_command(self, steering_command):
        """
        helper function for executing the Steering Commands.
        """
        self.__logger.info(f'Executing command: {steering_command.name}')
        # prepare the control command
        self.__prepare_contorl_command(steering_command)
        self.__logger.debug(f'sending the command: {self.__control_command.command}')
        # pickle and encode Control Command object
        control_command = multiprocess_utils.b64encode_and_pickle(
            self.__logger, self.__control_command)
        # 1. send steering command to C&C service
        if self.__communicator.send(
                control_command,
                self.__endpoint_with_command_control_service) ==\
                Response.ERROR:
            try:
                # Case a, something went wrong while sending
                # raise an exception
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

        # 3. process responses
        if self.__process_responses(responses, steering_command) == Response.ERROR:
            # Case a, something went wrong with Application Companions while
            #  executing
            # NOTE exception with traceback is already logged,
            # return with error
            return Response.ERROR

        # Case b, the command is executed successfully and everything went well
        self.__logger.debug(f'Successfully executed the command:'
                            f'{steering_command.name}')
        return Response.OK

    def __prepare_contorl_command(self, steering_command):
        """prepares the contorl command to send the Application Companion"""
        parameters = None
        # send global minimum step size with Steering command 'START'
        if steering_command == SteeringCommands.START:
            parameters = (self.__global_min_step_size, self.__spike_detectors)
        # prepare the control command
        self.__control_command.prepare(steering_command, parameters)
        self.__logger.debug("prepared the command: "
                            f"{self.__control_command.command}")
    
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

        Returns
        ------
        response code: int
            response code indicating whether the command is successfully executed
            and the global state is transitioned
        '''
        # i. check if the global state is valid for steering_command execution
        if self.current_global_state() != valid_state:
            self.__logger.critical(
                f'Global state must be {valid_state} for executing'
                f'the steering command: {steering_command}')
            return Response.ERROR

        # ii. update local state and state transition history
        state_before_transition = self.__orchestrator_registered_component.current_state
        self.__orchestrator_registered_component = self.__update_local_state(steering_command)
        if self.__orchestrator_registered_component == Response.ERROR:
            # local state could not be updated
            # NOTE an exception with traceback has already been logged in
            # callee function
            self.__logger.critical('Error updating the local state.')
            return Response.ERROR
        state_after_transition = self.__orchestrator_registered_component.current_state
        self.__health_registry_manager_proxy.update_state_transition_history(
            state_before_transition.name, steering_command.name,
            state_after_transition.name
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
        self.__logger.info(f'Global state now: {self.current_global_state()}')
        self.__logger.info(f'uptime till now: {self.up_time_till_now()}')
        return Response.OK

    def __register_with_registry(self):
        '''helper function to register with registry.'''
        if self.__health_registry_manager_proxy.register(
                        os.getpid(),  # id
                        SERVICE_COMPONENT_CATEGORY.ORCHESTRATOR,   # category
                        SERVICE_COMPONENT_CATEGORY.ORCHESTRATOR,   # name
                        self.__endpoints_address,  # endpoint
                        SERVICE_COMPONENT_STATUS.UP,  # current status
                        STATES.READY) == Response.ERROR:  # current state
            # Case, registration fails
            try:
                # raise run time error exception
                raise RuntimeError
            except RuntimeError:
                # log the exception with traceback
                self.__logger.exception('Could not be registered. Quitting!')
                # raise signal to terminate
                signal.raise_signal(signal.SIGTERM)
            # terminate with error
            return Response.ERROR

        # Case, registration is done
        # retrieve proxy to registered component which is later needed to
        # update the states
        self.__orchestrator_registered_component =\
            self.__health_registry_manager_proxy.find_by_id(os.getpid())
        self.__logger.debug(
            f'component service id: '
            f'{self.__orchestrator_registered_component.id}'
            f'; name: {self.__orchestrator_registered_component.name}')
        return Response.OK

    def __setup_endpoints(self):
        """creates communication endpoints"""
        # if the range of ports are not provided then use the shared queues
        # assuming that it is to be deployed on laptop/single node
        if self.__port_range is None:
            # proxies to the shared queues
            # for in-coming messages
            self.__endpoint_with_steering_service = multiprocessing.Manager().Queue()
            # for out-going messages
            self.__endpoint_with_command_control_service = multiprocessing.Manager().Queue()
            self.__endpoints_address = {
                # endpoint with Steering service to receive the commands
                SERVICE_COMPONENT_CATEGORY.STEERING_SERVICE:
                    self.__endpoint_with_steering_service,
                # endpoint with Command&Control service to send the commands
                SERVICE_COMPONENT_CATEGORY.COMMAND_AND_CONTROL:
                    self.__endpoint_with_command_control_service
                    }
            return Response.OK
        # Otherwise, create 0MQ sockets and bind with given port range
        else:
            # create ZMQ endpoints
            self.__zmq_sockets = ZMQSockets(self._log_settings, self._configurations_manager)
            # Endpoint with CLI for receiving commands via a REP socket
            self.__endpoint_with_steering_service = self.__zmq_sockets.create_socket(zmq.REP)
            # Endpoint with C&C service for sending commands via a REQ socket
            self.__endpoint_with_command_control_service = self.__zmq_sockets.create_socket(zmq.REQ)
            # get IP address
            self.__my_ip = networking_utils.my_ip()
            # get the port bound to REP socket to communicate with Steering
            # Service
            port_with_steering =\
                self.__zmq_sockets.bind_to_first_available_port(
                    zmq_socket=self.__endpoint_with_steering_service,
                    ip=self.__my_ip,
                    min_port=self.__port_range['MIN'],
                    max_port=self.__port_range['MAX'],
                    max_tries=self.__port_range['MAX_TRIES']
                )

            # check if port could not be bound
            if port_with_steering is Response.ERROR:
                # NOTE the relevant exception with traceback is already logged
                # by callee function
                self.__logger.error("port with Steering service could not be "
                                    "bound")
                # return with error to terminate
                return Response.ERROR

            # everything went well, now create endpoints address
            endpoint_address_with_steering_service = Endpoint(
                self.__my_ip, port_with_steering)
            self.__endpoints_address = {
                                SERVICE_COMPONENT_CATEGORY.STEERING_SERVICE:
                                endpoint_address_with_steering_service}

            return Response.OK

    def __setup_channel_with_command_control_service(self):
        """
        helper function to set up communication channels with steering CLI
        and C&C service
        """
        # 1. fetch proxy to C&C from registry
        self.__command_and_control_service =\
            self.__get_component_from_registry(
                        SERVICE_COMPONENT_CATEGORY.COMMAND_AND_CONTROL
                        )
        self.__logger.debug(f'command and steering service: '
                            f'{self.__command_and_control_service[0]}')

        if self.__command_and_control_service == Response.ERROR:
            self.__logger.critical('Proxy to Command and Control service is '
                                   'not found in registry')
            # return with error to terminate
            return Response.ERROR

        # 2. fetch C&C endpoint to communicate with Orchestrator
        self.__command_and_steering_service_endpoint =\
            self.__command_and_control_service[0].endpoint[
                SERVICE_COMPONENT_CATEGORY.ORCHESTRATOR]

        # 3. connect with endpoint (REP socket) of C&C service
        self.__endpoint_with_command_control_service.connect(
            f"tcp://"  # protocol
            f"{self.__command_and_steering_service_endpoint.IP}:"  # ip
            f"{self.__command_and_steering_service_endpoint.port}"  # port
            )
        self.__logger.info(
            "C&C channel - connected with C&C service to send commands at "
            f"{self.__command_and_steering_service_endpoint.IP}"
            f":{self.__command_and_steering_service_endpoint.port}")

        return Response.OK

    def __setup_communicator(self):
        """
        helper function to set up a generic communicator to encapsulate the
        underlying tech going to be used for communication.
        """
        if self.__port_range is None:  # communicate via shared queues
            self.__communicator = CommunicatorQueue(
                self._log_settings,
                self._configurations_manager)
        else:  # communicate via 0MQs
            self.__communicator = CommunicatorZMQ(
                self._log_settings,
                self._configurations_manager)

    def __setup_runtime(self):
        """
        helper function for setting up runtime such as register with registry,
        update global state, etc. before starting orchestration.
        """
        # 1. create endpoints for communications
        self.__setup_endpoints()

        # 2. register with registry
        if self.__register_with_registry() == Response.ERROR:
            # terminate with ERROR
            return Response.ERROR

        # 3. setup a communication channel with command control service
        if self.__setup_channel_with_command_control_service() == Response.ERROR:
            # terminate with ERROR
            return Response.ERROR

        # 4. initialize the Communicator object for communication via Queues
        self.__setup_communicator()

        # 5. iniitialize Control Command object
        self.__control_command = ControlCommand(self._log_settings,
                                                self._configurations_manager) 

        # 6. update global state
        if self.__update_global_state() == Response.ERROR:
            self.__logger.critical('Error updating the global state.')
            return Response.ERROR

        # 7. start monitoring threads
        self.__start_global_health_monitoring()

        # 8. all is setup now start orchestration
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
        # send signal to Proxy Manager Server to stop
        self.__logger.info('Stopping Proxy Manager Server')
        self._proxy_manager_client.stop_server()
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
                                                self.__endpoint_with_steering_service)
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
                        f'error executing: {current_steering_command}')
                # terminate loudly with error
                self.__communicator.send(
                    self.__terminate_with_error(),
                    self.__endpoint_with_steering_service)
                # self.__orchestrator_out_queue.put()
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

                # send signal to Proxy Manager Server to stop
                self.__logger.info('Stopping Proxy Manager Server')
                self._proxy_manager_client.stop_server()
                # finish execution as normal
                self.__logger.info('Concluding orchestration')
                # self.__orchestrator_out_queue.put(Response.OK)
                self.__communicator.send(Response.OK,
                                         self.__endpoint_with_steering_service)
                return Response.OK

            # Execution is not yet ended, fetch the next steering commands
            # self.__orchestrator_out_queue.put(Response.OK)
            self.__communicator.send(Response.OK,
                                     self.__endpoint_with_steering_service)
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
        self.__logger.info("running at hostname: "
                           f"{networking_utils.my_host_name()}, "
                           f"ip: {networking_utils.my_ip()}")
        # Setup runtime such as register with registry etc.
        if self.__setup_runtime() == Response.ERROR:
            # NOTE exceptions are already logged at source of failure
            self.__logger.error('Setting up runtime failed, Quitting!.')
            # terminate with error
            self.__communicator.send(Response.ERROR,
                                     self.__endpoint_with_steering_service)
            # self.__orchestrator_out_queue.put(Response.ERROR)
            return Response.ERROR

        # Runtime setup is done, start orchestration
        return self.__command_control_and_coordinate()


if __name__ == '__main__':
    if len(sys.argv)==5:
        # TODO better handling of arguments parsing
        
        # 1. unpickle objects
        # unpickle log_settings
        log_settings = pickle.loads(base64.b64decode(sys.argv[1]))
        # unpickle configurations_manager object
        configurations_manager = pickle.loads(base64.b64decode(sys.argv[2]))
        # unpickle connection details of Registry Proxy Manager object
        proxy_manager_connection_details = pickle.loads(base64.b64decode(sys.argv[3]))
        # unpickle range of ports for Orchestrator
        port_range = pickle.loads(base64.b64decode(sys.argv[4]))
        
        # 2. security check of pickled objects
        # it raises an exception, if the integrity is compromised
        try:
            check_integrity(configurations_manager, ConfigurationsManager)
            check_integrity(log_settings, dict)
            check_integrity(proxy_manager_connection_details, dict)
            check_integrity(port_range, dict)
        except Exception as e:
            # NOTE an exception is already raised with context when checking the 
            # integrity
            print(f'pickled object is not an instance of expected type!. {e}')
            sys.exit(1)

        # 3. all is well, instantiate COrchestrator
        orchestrator = Orchestrator(
            log_settings,
            configurations_manager,
            proxy_manager_connection_details,
            port_range
        )
        # 4. start executing Orchestrator
        orchestrator.run()
        sys.exit(0)
    else:
        print(f'missing argument[s]; required: 5, received: {len(sys.argv)}')
        print(f'Argument list received: {str(sys.argv)}')
        sys.exit(1)
