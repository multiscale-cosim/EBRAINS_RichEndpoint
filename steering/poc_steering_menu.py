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
from EBRAINS_RichEndpoint.Application_Companion.common_enums import Response
from EBRAINS_RichEndpoint.Application_Companion.common_enums import SteeringCommands
from EBRAINS_RichEndpoint.Application_Companion.common_enums import EVENT
from EBRAINS_RichEndpoint.orchestrator.communicator_zmq import CommunicatorZMQ
from EBRAINS_RichEndpoint.steering.steering_menu_handler import SteeringMenuCLIHandler
from EBRAINS_RichEndpoint.orchestrator.communicator_queue import CommunicatorQueue
from EBRAINS_RichEndpoint.orchestrator.zmq_sockets import ZMQSockets


class POCSteeringMenu:
    '''demonstrates the POC of steering via CLI.'''
    def __init__(self, log_settings,
                 configurations_manager,
                 orchestrator_in_queue,
                 orchestrator_out_queue,
                 communicate_via_zmqs = False):
        self._log_settings = log_settings
        self._configurations_manager = configurations_manager
        self.__logger = self._configurations_manager.load_log_configurations(
                                        name=__name__,
                                        log_configurations=self._log_settings)
        self.__steering_commands_history = []
        self.__current_legitimate_choice = 0
        self.__steering_menu_handler = SteeringMenuCLIHandler()
        
        if communicate_via_zmqs:  # communicate via 0MQs
            self.__communicator = CommunicatorZMQ(log_settings,
                                                  configurations_manager)
            self.__setup_channel_with_orchestrator(orchestrator_in_queue)
            self.__orchestrator_in_queue = self.__orchestrator_out_queue = self.__endpoint_with_orchestrator
        
        else:  # communicate via shared queues
            self.__communicator = CommunicatorQueue(log_settings,
                                                    configurations_manager)
            self.__orchestrator_in_queue = orchestrator_in_queue
            self.__orchestrator_out_queue = orchestrator_out_queue
       
        self.__logger.debug('initialized.')

    def __setup_channel_with_orchestrator(self, address):
        # create ZMQ endpoint
        self.__zmq_sockets = ZMQSockets(self._log_settings, self._configurations_manager)
        # Endpoint with C&C service for sending commands via a REQ socket
        self.__endpoint_with_orchestrator = self.__zmq_sockets.req_socket()
        # connect with endpoint (REP socket) of Orchestrator
        self.__endpoint_with_orchestrator.connect(
            f"tcp://"  # protocol
            f"{address.IP}:"  # ip
            f"{address.port}"  # port
            )
        self.__logger.info("C&C channel - connected with Orchestrator to send "
                           f"commands at {address.IP}:{address.port}")
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
        starts the steering menu handler to execute the user choice
        steering command.

        Parameters
        ----------
        orchestrator_component_in_queue : Queue
            Orchestrator queue for incoming messages.

        Returns
        ------
        response code as int
        '''
        # needed to check later if user enters a valid menu choice
        self.__current_legitimate_choice = 1

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

        # Case, INIT is done successfully
        user_choice = 0

        # Step 2. start receiving steering from user
        while True:
            # increment the index of currently valid menu choice
            self.__current_legitimate_choice += 1
            # display steering commands menu
            self.__steering_menu_handler.display_steering_menu()
            # get the user input
            user_choice = self.__steering_menu_handler.get_user_choice()
            user_choice += 1  # because INIT is already done as a system action
            # parse user choice
            user_choice =\
                self.__steering_menu_handler.parse_user_choice(user_choice)
            # terminate if user choice is 'Exit'
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

        # user opted for 'EXIT', terminate the steering menu
        return Response.OK
