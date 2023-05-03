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
#
# ------------------------------------------------------------------------------
import os
import zmq
import sys
import pickle
import base64

from common.utils.security_utils import check_integrity
from common.utils import networking_utils

from EBRAINS_RichEndpoint.application_companion.common_enums import Response
from EBRAINS_RichEndpoint.application_companion.common_enums import SteeringCommands
from EBRAINS_RichEndpoint.application_companion.common_enums import EVENT
from EBRAINS_RichEndpoint.application_companion.common_enums import SERVICE_COMPONENT_CATEGORY
from EBRAINS_RichEndpoint.application_companion.common_enums import SERVICE_COMPONENT_STATUS
from EBRAINS_RichEndpoint.orchestrator.proxy_manager_client import ProxyManagerClient
from EBRAINS_RichEndpoint.orchestrator.communicator_zmq import CommunicatorZMQ
from EBRAINS_RichEndpoint.steering.steering_menu_handler import SteeringMenuCLIHandler
from EBRAINS_RichEndpoint.orchestrator.communicator_queue import CommunicatorQueue
from EBRAINS_RichEndpoint.orchestrator.zmq_sockets import ZMQSockets

from EBRAINS_ConfigManager.global_configurations_manager.xml_parsers.configurations_manager import ConfigurationsManager


class SteeringService:
    '''demonstrates the POC of steering via CLI.'''
    def __init__(self, log_settings,
                 configurations_manager,
                 proxy_manager_connection_details,
                 is_communicate_via_zmqs=False,
                 is_interactive=False):
        self._log_settings = log_settings
        self._configurations_manager = configurations_manager
        self.__logger = self._configurations_manager.load_log_configurations(
                                        name="Steering_Service",
                                        log_configurations=self._log_settings)

        self._is_interactive = is_interactive
        self.__steering_commands_history = []
        self.__proxy_manager_client = None
        self.__current_legitimate_choice = 0
        self.__steering_menu_handler = SteeringMenuCLIHandler()

        # get proxy of registry manager
        self.__connect_to_registry_manager(proxy_manager_connection_details)

        # register with registry manager
        self.__register_with_registry()

        # set up communication channels with orchestrator
        self.__setup_channel_with_orchestrator(is_communicate_via_zmqs)

        self.__logger.debug('steering service initialized.')

    def __connect_to_registry_manager(self,proxy_manager_connection_details):
        """ needed to check if all necessary components are registered """
        # get client to Proxy Manager Server
        self.__proxy_manager_client = ProxyManagerClient(
            self._log_settings,
            self._configurations_manager)
        # Connect with Proxy Manager Server
        # NOTE: it terminates with RuntimeError if connection could ne be made
        # for whatever reasons
        self.__proxy_manager_client.connect(
            proxy_manager_connection_details["IP"],
            proxy_manager_connection_details["PORT"],
            proxy_manager_connection_details["KEY"],
        )   
        # get proxy to registry manager
        self.__health_registry_manager_proxy =\
            self.__proxy_manager_client.get_registry_proxy()
        return Response.OK

    def __register_with_registry(self):
        """
        helper function for registering with registry service
        """
        # register with registry
        if (self.__health_registry_manager_proxy.register(
                os.getpid(), # id
                SERVICE_COMPONENT_CATEGORY.STEERING_SERVICE,  # name
                SERVICE_COMPONENT_CATEGORY.STEERING_SERVICE,  # category
                None,  # endpoint (needed if another component wants to connect)
                SERVICE_COMPONENT_STATUS.UP,  # current status
                None) == Response.OK):  # current state

            self.__logger.info('Steering service is registered.')
            return Response.OK
        else:
            return self.__terminate_with_error("Steering Service could not be registered")

    def __setup_channel_with_orchestrator(self, is_communicate_via_zmqs):
        """
        sets up C&C channels with orchestrator, ZMQ or shared queues.

        Parameters
        ----------
        is_communicate_via_zmqs: bool
            indicates whether the communication is via 0MQs
        """
        # get proxy to Orchestrator registered component
        orchestrator_component = self.__health_registry_manager_proxy.\
            find_all_by_category(SERVICE_COMPONENT_CATEGORY.ORCHESTRATOR)

        # set up communiation channel with Orchestrator
        # Case a, setup communication channel using Shared Queues
        if not is_communicate_via_zmqs:
            # set in- and out-queue
            orchestrator_in_queue = \
                orchestrator_component[0].endpoint[SERVICE_COMPONENT_CATEGORY.STEERING_SERVICE]
            orchestrator_out_queue = \
                orchestrator_component[0].endpoint[SERVICE_COMPONENT_CATEGORY.COMMAND_AND_CONTROL]
            self.__communicator = CommunicatorQueue(self._log_settings,
                                                    self._configurations_manager)
            self.__orchestrator_in_queue = orchestrator_in_queue
            self.__orchestrator_out_queue = orchestrator_out_queue

        # Case b, setup communication channel using 0MQs
        else:
            orchestrator_endpoint = \
                orchestrator_component[0].endpoint[SERVICE_COMPONENT_CATEGORY.STEERING_SERVICE]
            # create communicator and set up channels with orchestrator
            self.__communicator = CommunicatorZMQ(self._log_settings,
                                                  self._configurations_manager)
            # create ZMQ endpoint
            zmq_sockets = ZMQSockets(self._log_settings, self._configurations_manager)
            # Endpoint with C&C service for sending commands via a REQ socket
            req_endpoint_with_orchestrator = zmq_sockets.create_socket(zmq.REQ)
            # connect with endpoint (REP socket) of Orchestrator
            req_endpoint_with_orchestrator.connect(
                f"tcp://"  # protocol
                f"{orchestrator_endpoint.IP}:"  # ip
                f"{orchestrator_endpoint.port}"  # port
                )
            self.__logger.info("C&C channel - connected with Orchestrator to send "
                           f"commands at {orchestrator_endpoint.IP}:{orchestrator_endpoint.port}")
            self.__orchestrator_in_queue = self.__orchestrator_out_queue = req_endpoint_with_orchestrator

        return Response.OK

    def __get_steering_menu_item(self, command):
        """
        maps the steering command with menu item.

        Parameters
        ----------
        command : SteeringCommands.Enum
            steering command enum

        Returns
        ------
        menu item: returns the corresponding menu item, if found.
        None: if not found.
        """
        menu_item = self.__steering_menu_handler.get_menu_item(command)
        if menu_item != Response.ERROR:
            return menu_item
        else:
            return None

    def __validate_steering_command(self, command):
        '''validates the steering command before sending to Orchestrator.'''
        self.__logger.debug(
            f'legitimate choice: {self.__current_legitimate_choice}'
            f' current user choice:{command}')
        return (command == self.__current_legitimate_choice
                or command == SteeringCommands.END)

    def __send_steering_command_to_orchestrator(self, command):
        '''helper function to send the steering command to Orchestrator.'''
        self.__logger.info(f'sending command {command.name} to Orchestrator.')
        self.__communicator.send(command, self.__orchestrator_in_queue)

    def __execute_if_validated(self, user_choice):
        '''
        helper function to execute the steering commands after validation.

        NOTE this is a client side validation, so nothing to do with the
        state machine valid states and rules.
        '''
        # 1. check if the user choice is a not valid menu choice
        # e.g. it does not exist in steering menu
        if not self.__validate_steering_command(user_choice):
            # Case a. user choice is not a valid choice
            return Response.NOT_VALID_COMMAND

        # Case b. user choice is a valid choice
        # 2. send the steering command to Orchestrator
        self.__send_steering_command_to_orchestrator(user_choice)

        # 3. keep track of steering commands
        self.__steering_commands_history.append(
            self.__get_steering_menu_item(user_choice))

        # 4. get response from Orchestrator
        response = self.__get_responses()

        # 5. parse and return the response
        return self.__parse_response(response)

    def __get_responses(self):
        '''
        helper function for receiving the responses from Orchestrator
        '''
        try:
            self.__logger.debug('getting response from Orchestrator.')
            return self.__communicator.receive(self.__orchestrator_out_queue)
        except Exception:
            # Log the exception with Traceback details
            self.__logger.exception('exception while getting response.')
            return Response.ERROR

    def __parse_response(self, response):
        self.__logger.debug(f'response from orchestrator: {response}')
        if response == Response.ERROR or response == EVENT.FATAL:
            # terminate with error
            return self.__terminate_with_error('Error executing command. '
                                               'Quitting!')
        else:
            return response

    def __terminate_with_error(self, error_message):
        try:
            # raise run time error exception
            raise RuntimeError
        except RuntimeError:
            # log the exception with traceback
            self.__logger.exception(error_message)
        # terminate with error
        return Response.ERROR

    def start_steering(self):
        '''
        starts the steering menu handler 
        - if interactive: display steering menu and execute the user choice
        - if non interactive: execute steering commands automatically

        Parameters
        ----------
        orchestrator_component_in_queue : Queue
            Orchestrator queue for incoming messages.

        Returns
        ------
        response code as int
        '''
        self.__logger.info("running at hostname: "
                           f"{networking_utils.my_host_name()}, "
                           f"ip: {networking_utils.my_ip()}")
        # Step 1. send INIT command to Orchestrator
        # NOTE INIT is a system action and thus is done implicitly
        self.__send_steering_command_to_orchestrator(SteeringCommands.INIT)
        # keep track of steering actions
        self.__steering_commands_history.append('Init')
        response = self.__get_responses()
        self.__logger.debug(f'response from orchestrator: {response}')
        if response == Response.ERROR:
            # terminate with error
            return self.__terminate_with_error('Error executing INIT command. '
                                               'Quitting!')

        # Step 2. non-system actions
        self.__current_legitimate_choice = 1  # currently: 1 = INIT
        # Step 2a. interactive mode with steering menu
        if self._is_interactive:
            # NOTE WARNING did not test on supercomputers, may be broken
            # TODO test it on supercomputers
            user_choice = 0
            while True:
                # increment the index of currently valid menu choice
                self.__current_legitimate_choice += 1
                # display steering commands menu
                self.__steering_menu_handler.display_steering_menu()
                # get the user input
                user_choice = self.__steering_menu_handler.get_user_choice()
                user_choice += 1  # add 1 because INIT=1 is already done as a system action
                # parse user choice
                user_choice =\
                    self.__steering_menu_handler.parse_user_choice(user_choice)
                # terminate if current choice is 'Exit'
                if user_choice == SteeringCommands.EXIT:
                    print("Exiting...")
                    break

                # Otherwise, execute the steering command if it is a valid
                # menu choice
                response = self.__execute_if_validated(user_choice)
                if response == Response.NOT_VALID_COMMAND:
                    print('Not a valid choice. The valid choices are: [1-3]')
                    continue
        # Step 2b. non interactive mode, execute menu items in order
        else:
            # NOTE assumes only non-system actions and correct order/enum in menu
            # e.g. SteeringCommands.START=2, END=3, EXIT=4, ...
            for current_choice in self.__steering_menu_handler.all_steering_commands:
                # terminate if current_choice is 'Exit'
                if current_choice == SteeringCommands.EXIT:
                    print("Concluding the Steering Menu Service")
                    break  # if EXIT is not already the last menu item

                # SAFETY check: increment the index of currently valid menu
                # choice
                self.__current_legitimate_choice += 1
                response = self.__execute_if_validated(current_choice)
                # no check if response==Response.NOT_VALID_COMMAND

        # SteeringCommands.EXIT executed --> terminate the steering (menu)
        return Response.OK


if __name__ == '__main__':
    if len(sys.argv) == 6:
        # TODO better handling of arguments parsing

        # 1. unpickle objects
        # unpickle log_settings
        log_settings = pickle.loads(base64.b64decode(sys.argv[1]))
        # unpickle configurations_manager object
        configurations_manager = pickle.loads(base64.b64decode(sys.argv[2]))
        # unpickle connection details of Registry Proxy Manager object
        proxy_manager_connection_details = pickle.loads(base64.b64decode(sys.argv[3]))
        # flag to indicate the communication channel e.g. 0MQ or Shared Queues
        is_communicate_via_zmqs = pickle.loads(base64.b64decode(sys.argv[4]))
        # flag to indicate whether the steering is interactive
        # is_interactive = pickle.loads(base64.b64decode(sys.argv[5]))
        is_interactive = pickle.loads(base64.b64decode(sys.argv[5]))

        # 2. security check of pickled objects
        # it raises an exception, if the integrity is compromised
        try:
            check_integrity(configurations_manager, ConfigurationsManager)
            check_integrity(log_settings, dict)
            check_integrity(proxy_manager_connection_details, dict)
            check_integrity(is_communicate_via_zmqs, bool)
            check_integrity(is_interactive, bool)
        except Exception as e:
            # NOTE an exception is already raised with context when checking
            # the integrity
            print('pickled object is not an instance of expected type!.')
            print(f'Traceback: {e}')
            sys.exit(1)

        # 3. all is well, instantiate Steering Service
        steering_service = SteeringService(
            log_settings,
            configurations_manager,
            proxy_manager_connection_details,
            is_communicate_via_zmqs,
            is_interactive
        )
        # 4. start executing Steering Service
        steering_service.start_steering()
        sys.exit(0)
    else:
        print(f'missing argument[s]; required: 6, received: {len(sys.argv)}')
        print(f'Argument list received: {str(sys.argv)}')
        sys.exit(1)
