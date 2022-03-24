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
from EBRAINS_RichEndpoint.Application_Companion.common_enums import SteeringCommands


class SteeringMenuCLI:
    '''
    Menu class for interacting with the user via CLI.
    '''

    def __init__(self) -> None:
        # setup a dictionary for menu handling as a key:value pair
        # i.e. SteeringCommands.Enum: str (to display)
        self.__steering_menu = {
            # INIT is a system action and so is done implicitly
            # SteeringCommands.INIT: "Setup synchronization",
            SteeringCommands.START: "Start",
            SteeringCommands.END: "End",
            SteeringCommands.EXIT: "Exit"
        }
        self.__steering_menu_items = self.__steering_menu.keys()

    @property
    def steering_menu_items(self): return self.__steering_menu_items

    @property
    def steering_menu(self): return self.__steering_menu
