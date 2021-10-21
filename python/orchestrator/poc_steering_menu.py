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
from python.Application_Companion.common_enums import Response
from python.Application_Companion.common_enums import SteeringCommands
from python.orchestrator.steering_menu_handler import SteeringMenuCLIHandler
from python.orchestrator.communicator_queue import CommunicatorQueue


class POCSteeringMenu:
    '''demonstrates the POC of steering via CLI.'''
    def __init__(self, log_settings, configurations_manager) -> None:
        self._log_settings = log_settings
        self._configurations_manager = configurations_manager
        self.__logger = self._configurations_manager.load_log_configurations(
                                        name=__name__,
                                        log_configurations=self._log_settings)
        self.__steering_commands_history = []
        self.__current_legitimate_choice = 0
        self.__steering_menu_handler = SteeringMenuCLIHandler()
        self.__communicator = CommunicatorQueue(log_settings,
                                                configurations_manager)
        self.__logger.debug('initialized.')

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

    def __get_responses(self, orchestrator_component_out_queue):
        '''
        helper function for receiving the responses from Orchestrator
        '''
        try:
            self.__logger.debug('getting response from Orchestrator.')
            return self.__communicator.receive(
                    orchestrator_component_out_queue)
        except Exception:
            # Log the exception with Traceback details
            self.__logger.exception('exception while getting response.')
            return Response.ERROR

    def __validate_steering_command(self, command):
        '''validates the steering command before sending to Orchestrator.'''
        return (command == self.__current_legitimate_choice
                or command == SteeringCommands.END)

    def start_steering(self,
                              orchestrator_component_in_queue,
                              orchestrator_component_out_queue):
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
        user_choice = 0
        self.__current_legitimate_choice = 1
        while True:
            self.__steering_menu_handler.display_steering_menu()
            # get the user input
            user_choice = self.__steering_menu_handler.parse_user_choice(
                self.__steering_menu_handler.get_user_choice())
            # keep track of steering commands
            self.__steering_commands_history.append(
                            self.__get_steering_menu_item(
                                user_choice))
            if not(user_choice == Response.ERROR or
                    user_choice == SteeringCommands.EXIT):
                # validate if user choice is a valid steering command
                if self.__validate_steering_command(user_choice):
                    # send the steering command to Orchestrator
                    orchestrator_component_in_queue.put(user_choice)
                    self.__logger.debug(
                        f'response from orchestrator: '
                        f'{self.__get_responses(orchestrator_component_out_queue)}')
                    self.__current_legitimate_choice += 1
                else:
                    self.__logger.error(
                        f'The legitimate choices are: '
                        f'{self.__get_steering_menu_item(self.__current_legitimate_choice)}, '
                        f'{self.__get_steering_menu_item(SteeringCommands.END)},'
                        f' and EXIT.')
            elif user_choice == Response.ERROR:
                print("\nNot a valid choice. Enter again!")
            elif user_choice == SteeringCommands.EXIT:
                print(f'\nSteering command history: '
                      f'{self.__steering_commands_history}')
                print("Exiting...")
                break
        return Response.OK
