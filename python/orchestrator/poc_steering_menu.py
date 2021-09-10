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


class POCSteeringMenu:
    '''demonstrates the POC of steering via CLI.'''
    def __init__(self) -> None:
        self.__steering_commands_history = []
        self.__steering_menu_handler = SteeringMenuCLIHandler()

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

    def start_menu_handler(self, orchestrator_component_in_queue):
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
                # send the steering command to Orchestrator
                orchestrator_component_in_queue.put(user_choice)
            elif user_choice == Response.ERROR:
                print("\nNot a valid choice. Enter again!")
            elif user_choice == SteeringCommands.EXIT:
                print(f'\nSteering command history: '
                      f'{self.__steering_commands_history}')
                print("Exiting.")
                break
        return Response.OK
