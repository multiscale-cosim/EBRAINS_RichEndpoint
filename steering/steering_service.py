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
import zmq

from EBRAINS_RichEndpoint.Application_Companion.common_enums import Response
from EBRAINS_RichEndpoint.Application_Companion.common_enums import SteeringCommands
from EBRAINS_RichEndpoint.Application_Companion.common_enums import EVENT
from EBRAINS_RichEndpoint.Application_Companion.common_enums import SERVICE_COMPONENT_CATEGORY
from EBRAINS_RichEndpoint.orchestrator.proxy_manager_client import ProxyManagerClient
from EBRAINS_RichEndpoint.orchestrator.communicator_zmq import CommunicatorZMQ
from EBRAINS_RichEndpoint.steering.steering_menu_handler import SteeringMenuCLIHandler
from EBRAINS_RichEndpoint.orchestrator.communicator_queue import CommunicatorQueue
from EBRAINS_RichEndpoint.orchestrator.zmq_sockets import ZMQSockets


class SteeringService:
    '''demonstrates the POC of steering via CLI.'''
    def __init__(self, log_settings,
                 configurations_manager,
                 proxy_manager_connection_details,
                 ports_for_cc_channel,
                 num_app_companions,
                 communicate_via_zmqs = False,
                 is_interactive = False):
        self._log_settings = log_settings
        self._configurations_manager = configurations_manager
        self._is_interactive = is_interactive
        self.__logger = self._configurations_manager.load_log_configurations(
                                        name=__name__,
                                        log_configurations=self._log_settings)
        self.__steering_commands_history = []
        self.__current_legitimate_choice = 0
        self.__steering_menu_handler = SteeringMenuCLIHandler()
        
        # 1) get proxy to registry manager
        self.__connect_to_registry_manager(proxy_manager_connection_details)
        
        # 2) check if all necessary components are registered
        self.__check_registered_components(num_app_companions)
        
        # 3) set up communication channels with orchestrator
        self.__setup_channel_with_orchestrator(ports_for_cc_channel, communicate_via_zmqs)
       
        self.__logger.debug('steering service initialized.')
    
    def __connect_to_registry_manager(self,proxy_manager_connection_details):
        """ needed to check if all necessary components are registered """
        # get client to Proxy Manager Server
        proxy_manager_client = ProxyManagerClient(
            self._log_settings,
            self._configurations_manager)
        # Connect with Proxy Manager Server
        # NOTE: it terminates with RuntimeError if connection could ne be made
        # for whatever reasons
        proxy_manager_client.connect(
            proxy_manager_connection_details["IP"],
            proxy_manager_connection_details["PORT"],
            proxy_manager_connection_details["KEY"],
        )   
        # get proxy to registry manager
        self.__registry_manager_proxy = proxy_manager_client.get_registry_proxy()
        return Response.OK
    
    def __check_registered_components(self,num_app_comp):
        """
        Check if registered with registry service:
        APPLICATION_COMPANION (one per action), ORCHESTRATOR, COMMAND_AND_CONTROL
        APPLICATION_MANAGER is checked within app.companion
        """
        if len(self.__get_component_from_registry(
                SERVICE_COMPONENT_CATEGORY.APPLICATION_COMPANION)) < num_app_comp:
            self.__logger.exception('Not all Application Companions are yet registered')
        # orchestrator component needed for c&c channel setup
        self.__orchestrator_component = self.__get_component_from_registry(
                SERVICE_COMPONENT_CATEGORY.ORCHESTRATOR)
        if self.__orchestrator_component is None:
            self.__logger.exception('ORCHESTRATOR is not yet registered')
        if self.__get_component_from_registry(
                SERVICE_COMPONENT_CATEGORY.COMMAND_AND_CONTROL) is None:
            self.__logger.exception('Command&Control service is not yet registered')
        # all necessary components registered
        self.__logger.debug('all necessary components registered')
        return Response.OK

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
        #TODO implement retry-loop and timeout --> see previous check in launcher.py
        components = self.__registry_manager_proxy.\
            find_all_by_category(target_components_category)
        self.__logger.debug(
                f'found {len(components)} component(s) \
                in category {target_components_category}')
        return components

    def __setup_channel_with_orchestrator(self, ports_for_cc_channel, communicate_via_zmqs):
        """
        Set orchestrator endpoints for communication. Requires registered orchestrator component.
        setup C&C channels with orchestrator, ZMQ or shared queues.
        Parameters
        ----------
        ports_for_cc_channel : dict
            portrange values, read/set by launcher
        communicate_via_zmqs: bool
            determines type of communication/steering channel
        """
        # set in- and out-queue
        if ports_for_cc_channel is None:
            orchestrator_in_queue = \
                self.__orchestrator_component[0].endpoint[SERVICE_COMPONENT_CATEGORY.STEERING_SERVICE]
            orchestrator_out_queue = \
                self.__orchestrator_component[0].endpoint[SERVICE_COMPONENT_CATEGORY.COMMAND_AND_CONTROL]
        else:
            orchestrator_in_queue = orchestrator_out_queue = \
                self.__orchestrator_component[0].endpoint[SERVICE_COMPONENT_CATEGORY.STEERING_SERVICE]
        # create communicator and set up channels with orchestrator
        if communicate_via_zmqs:  # communicate via 0MQs
            self.__communicator = CommunicatorZMQ(self._log_settings,
                                                  self._configurations_manager)
            # create ZMQ endpoint
            zmq_sockets = ZMQSockets(self._log_settings, self._configurations_manager)
            # Endpoint with C&C service for sending commands via a REQ socket
            endpoint_with_orchestrator = zmq_sockets.create_socket(zmq.REQ)
            # connect with endpoint (REP socket) of Orchestrator
            endpoint_with_orchestrator.connect(
                f"tcp://"  # protocol
                f"{orchestrator_in_queue.IP}:"  # ip
                f"{orchestrator_in_queue.port}"  # port
                )
            self.__logger.info("C&C channel - connected with Orchestrator to send "
                           f"commands at {orchestrator_in_queue.IP}:{orchestrator_in_queue.port}")
            self.__orchestrator_in_queue = self.__orchestrator_out_queue = endpoint_with_orchestrator
        else:  # communicate via shared queues
            self.__communicator = CommunicatorQueue(self._log_settings,
                                                    self._configurations_manager)
            self.__orchestrator_in_queue = orchestrator_in_queue
            self.__orchestrator_out_queue = orchestrator_out_queue
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

    def send_steering_command_to_orchestrator(self, command):
        '''helper function to send the steering command to Orchestrator.'''
        self.__logger.debug(f'sending: {command} to Orchestrator.')
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
        self.send_steering_command_to_orchestrator(user_choice)

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

        # Step 1. send INIT command to Orchestrator
        # NOTE INIT is a system action and thus is done implicitly
        self.send_steering_command_to_orchestrator(SteeringCommands.INIT)
        # keep track of steering actions
        self.__steering_commands_history.append('Init')
        response = self.__get_responses()
        self.__logger.debug(f'response from orchestrator: {response}')
        if response == Response.ERROR:
            # terminate with error
            return self.__terminate_with_error('Error executing INIT command. '
                                               'Quitting!')
        
        # Step 2. non-system actions
        self.__current_legitimate_choice = 1 # currently: 1 = INIT
        # Step 2a. interactive mode with steering menu
        if self._is_interactive:
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
                    print(f'Steering command history: '
                       f'{self.__steering_commands_history}')
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
                    print(f'Steering command history: '
                        f'{self.__steering_commands_history}')
                    print("Exiting...")
                    break # if EXIT is not already the last menu item

                # SAFETY check: increment the index of currently valid menu choice
                self.__current_legitimate_choice += 1
                response = self.__execute_if_validated(current_choice)
                # no check if reponse==Response.NOT_VALID_COMMAND

        # SteeringCommands.EXIT executed --> terminate the steering (menu)
        return Response.OK
