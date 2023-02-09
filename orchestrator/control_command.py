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
from EBRAINS_RichEndpoint.application_companion.common_enums import Response
from EBRAINS_RichEndpoint.application_companion.common_enums import COMMANDS


class ControlCommand:
    """
        Provides methods to prepare and parse control command object which
        comprises of steering command and parameters.
    """
    def __init__(self, log_settings, configurations_manager):
        self.__logger = configurations_manager.load_log_configurations(
            name="ControlCommand",
            log_configurations=log_settings)
        self.__command = {}
        self.__logger.debug("initialized")
    
    @property
    def command(self): return self.__command

    def prepare(self, steering_command, parameters):
        """
            prepares the control command that comprises of steering command
            and parameters
        """
        self.__command = {COMMANDS.STEERING_COMMAND.name: steering_command,
                COMMANDS.PARAMETERS.name: parameters}
        self.__logger.debug(f"prepared the command: {self.__command}")
        return self.__command

    def update_paramters(self, parameters):
        """
           updates parameters in current control command
        """
        self.__logger.debug(f"original command: {self.__command}")
        # update the value of COMMANDS.PARAMETERS
        self.__command.update({COMMANDS.PARAMETERS.name: parameters})
        self.__logger.debug(f"command after update: {self.__command}")
    
    def parse(self):
        """
            parses control command and returns a tuple of steering command
            and parameters
        """
        self.__logger.debug(f"command: {self.__command}")
        try:
            current_steering_command = self.__command.get(COMMANDS.STEERING_COMMAND.name)
            parameters = self.__command.get(COMMANDS.PARAMETERS.name)
        except ValueError:
            return self.__terminate_with_error_loudly("error in parsing command")
        
        return current_steering_command, parameters

    def __terminate_with_error_loudly(custom_message):
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