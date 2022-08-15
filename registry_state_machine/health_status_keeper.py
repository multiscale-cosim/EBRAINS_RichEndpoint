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
from datetime import datetime
from EBRAINS_RichEndpoint.Application_Companion.common_enums import Response
from EBRAINS_RichEndpoint.registry_state_machine.health_status import HealthStatus
from EBRAINS_RichEndpoint.registry_state_machine.state_enums import STATES


class HealthStatusKeeper:
    """
    Manages the  overall health and validity of the workflow.
    """
    def __init__(self, log_settings, configurations_manager) -> None:
        self._log_settings = log_settings
        self._configurations_manager = configurations_manager
        self.__logger = self._configurations_manager.load_log_configurations(
                                        name=__name__,
                                        log_configurations=self._log_settings)

        # instantiate the health and status data object,
        self.__health_status = HealthStatus()
        self.__logger.debug("initialized.")

    def __update_last_health_updated(self):
        """"updates the health check times stamp."""
        self.__health_status.last_updated = datetime.now()
        self.__logger.debug(f"last health check updated to "
                            f"{self.__last_health_check()}.")

    def __transit_global_state(self, state):
        """"helper function to update the global state."""
        self.__health_status.current_global_state = state
        self.__logger.info(f'global state is transited to '
                           f'{self.__health_status.current_global_state}')

    def __last_health_check(self):
        """returns the timestamp when the health and status is last updated."""
        return self.__health_status.last_updated
    
    def current_global_status(self):
        """returns the current global status of the System."""
        return self.__health_status.current_global_status

    def current_global_state(self):
        """returns the current global state of the System."""
        return self.__health_status.current_global_state

    def uptime_till_now(self):
        """
        returns the running uptime of the system since the start.
        """
        uptime_till_now = datetime.now() - self.__health_status.uptime
        self.__logger.debug(f"up time till now: {uptime_till_now}")
        return uptime_till_now

    def initialize_global_state(self, initial_state):
        self.__transit_global_state(initial_state)
    
    def update_global_state(self, new_global_state):
        """"
        updates the global state if all local states are same.
        """
        # self.__logger.debug(f"current global state: '"
        #                     f"{self.current_global_state()}'")
        self.__logger.debug(f"updating to: '{new_global_state}'")
        # update the global state as the current local state
        # of the components
        self.__transit_global_state(new_global_state)
        # update the last check timestamp
        self.__update_last_health_updated()
        self.__logger.debug(f"new global state: '{self.current_global_state()}'"
                            f" updated at {self.__last_health_check()}")
        return Response.OK

    
    # def update_global_state(self):
    #     """"
    #     It updates the global state if all local states are same.
    #     It returns an int code indicating whether or not the state is updated.
    #     """
    #     all_components, components_with_states =\
    #         self.__health_status_monitor.validate_local_states()
    #     # Case all components are UP and in the same state
    #     if all_components and components_with_states:
    #         # update the global state as the current local state
    #         # of the componenets
    #         self.__update_state(components_with_states[0].current_state)
    #         # update the last check timestamp
    #         self.__update_last_health_updated()
    #         self.__logger.debug("global state is updated.")
    #         return Response.OK
    #     else:
    #         self.__update_state(STATES.ERROR)
    #         # update the last check timestamp
    #         self.__update_last_health_updated()
    #         return Response.ERROR

    
