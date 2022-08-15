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
from EBRAINS_RichEndpoint.registry_state_machine.state_enums import STATES
from EBRAINS_RichEndpoint.Application_Companion.common_enums import SteeringCommands


class StateTransitionValidator:
    """
    Validates the local and global states as per transition rules.
    """
    def __init__(self, log_settings, configurations_manager) -> None:
        self._log_settings = log_settings
        self._configurations_manager = configurations_manager
        self.__logger = self._configurations_manager.load_log_configurations(
                                        name=__name__,
                                        log_configurations=self._log_settings)
        self.__logger.debug("initialized.")

    def next_valid_local_state(self, current_state, input_command):
        """
        Checks whether the constraints are met for local state transition and
        returns the next valid local state as per transition rules.

        Parameters
        ----------
        current_state: STATES.Enum
            current state

        input_command : SteeringCommands.Enum
            The input to transit from current state to next valid state

        Returns
        ------
        next_valid_state: STATES.Enum
            returns next valid state if the input provided is valid transit
            from current state

        ERROR: RESPONSE.Enum
            returns Error if the input provided is not valid to transit from
            current state
        """
        # Case: Transit from state 'READY' with input command 'INIT'
        # Rule: (READY, {INIT}) -> SYNCHRONIZING
        if current_state == STATES.READY and input_command == SteeringCommands.INIT:
            return STATES.SYNCHRONIZING

        # Case: Transit from state 'SYNCHRONIZING' with input command 'START'
        # Rule: (SYNCHRONIZING, {START}) -> RUNNING
        if current_state == STATES.SYNCHRONIZING and input_command == SteeringCommands.START:
            return STATES.RUNNING

        # Case: Transit from state 'RUNNING' with input command 'PAUSE'
        # Rule: (RUNNING, {PAUSE}) -> PAUSED
        if current_state == STATES.RUNNING and input_command == SteeringCommands.PAUSE:
            return STATES.PAUSED

        # Case: Transit from state 'RUNNING' with input command 'END'
        # Rule: (RUNNING, {END}) -> TERMINATED
        if current_state == STATES.RUNNING and input_command == SteeringCommands.END:
            return STATES.TERMINATED

        # Case: Transit from state 'PAUSED' with input command 'RESUME'
        # Rule: (PAUSED, {RESUME}) -> RUNNING
        if current_state == STATES.PAUSED and input_command == SteeringCommands.RESUME:
            return STATES.RUNNING
        
        # Case: unknown transition rule i.e. the command is not valid for the
        # current state
        else:
            return STATES.ERROR

    def next_valid_global_state(self, all_components, components_with_states,
                                are_all_statuses_up, are_all_have_same_state):
        """
        Checks whether the constraints are met for global state transition and
        returns the next valid global state as per transition rules.

        Parameters
        ----------
        all_components : ServiceComponent
            components which are currently registered in registry service

        components_with_states: ServiceComponent
            components which have states

        are_all_statuses_up: call_back function
            call back function which returns boolean value indicating whether
            or not the statuses of all components are 'UP'

        are_all_have_same_state: call_back function
            call back function which returns boolean value indicating whether
            or not the local states of all components are same

        Returns
        -------
        next_valid_state: STATES.Enum
            next valid global state

        """
        # NOTE: Transition rule to update global state
        
        #   CONSTRAINTS: 1) all local statuses must be 'UP'.
        #                2) all local states must be the same.
        
        #   RULES:
        #   1) if ALL Constraints ARE met then
        #   'the Global state is the same as the local state of a given component'
        #   2) if ANY constraint is NOT met then
        #   'the Global state is ERROR'
        
        # STEPS:
        # 1) check whether the constrains are met
        # 1.1) CONSTRAINT 1: if all local statuses are 'UP'
        # check with call_back function if all local statuses are 'UP'
        if not are_all_statuses_up(all_components):
            # Case 1: Constraint 1 is not satisfied - some component is DOWN
            self.__logger.error('some components are DOWN.'
                                ' So, transit Global state to ERROR')
            # next valid state is 'ERROR' as per rule
            return STATES.ERROR

        # Case 2: Constraint 1 is satisfied - all components are 'UP'
        self.__logger.debug("Constraint 1 is satisfied - all components are 'UP'")

        # 1.2) CONSTRAINT 2: if all local states are same
        # check with call_back function if all local states are same
        if not are_all_have_same_state(components_with_states):
            # Case 1: Constraint 2 is not satisfied - components have different states
            # log the exception with traceback
            self.__logger.error('components have different states.'
                                ' So, transit Global state to ERROR')
            # next valid state is 'ERROR' as per rule
            return STATES.ERROR

        # Case 2: Constraint 2 is satisfied - all components have same state
        self.__logger.debug('Constraint 2 is satisfied - all components have'
                            f' same state: {components_with_states[0].current_state}')

        # 3) Both constraints are satisfied. Now, return the next valid global
        # state
        self.__logger.debug(f'next valid global state: '
                            f'{components_with_states[0].current_state}')
        return components_with_states[0].current_state
