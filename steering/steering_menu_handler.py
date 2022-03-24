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
from EBRAINS_RichEndpoint.steering.steering_menu_cli import SteeringMenuCLI
from EBRAINS_RichEndpoint.Application_Companion.common_enums import Response


class SteeringMenuCLIHandler:
    '''
    Manages the Menu related functionality.
    NOTE: It is a POC as of now, and later will be extended to may be
    a separate process/thread.
    '''
    def __init__(self) -> None:
        self.__steering_menu_cli = SteeringMenuCLI()
        self.__current_choice = None

    @property
    def current_selection(self): return self.__current_choice

    def display_steering_menu(self):
        print('\n'+'*' * 33, flush=True)
        print('*\t Steering Menu \t\t*')
        print('*' * 33)
        index = 1
        for item in self.__steering_menu_cli.steering_menu_items:
            print(f'{index}. {self.__steering_menu_cli.steering_menu[item]} ')
            index += 1
        print('\n')

    def get_user_choice(self):
        choice = input("please enter the choice number [1-3]: ")
        self.__current_choice = self.__convert_str_to_int(choice)
        return self.current_selection

    def __convert_str_to_int(self, val_str):
        try:
            val_int = int(val_str)
            return val_int
        except ValueError:
            return Response.ERROR

    def parse_user_choice(self, user_choice):
        if user_choice in self.__steering_menu_cli.steering_menu_items:
            return user_choice
        else:
            return Response.ERROR

    def get_menu_item(self, item):
        if item in self.__steering_menu_cli.steering_menu_items:
            return self.__steering_menu_cli.steering_menu[item]
        else:
            return Response.ERROR
