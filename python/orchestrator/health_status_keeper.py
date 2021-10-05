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
from python.orchestrator.health_status import HealthStatus
from python.orchestrator.health_status_monitor import HealthStatusMonitor
from python.Application_Companion.common_enums import Response
from python.orchestrator.state_enums import STATES


class HealthStatusKeeper:
    """
    i)  Manages the global state transitions, and
    ii) Monitors the overall health and validity of the workflow.
    """
    def __init__(self, log_settings, configurations_manager,
                 service_registry_manager) -> None:
        self._log_settings = log_settings
        self._configurations_manager = configurations_manager
        self.__logger = self._configurations_manager.load_log_configurations(
                                        name=__name__,
                                        log_configurations=self._log_settings)
        # proxy to registry service
        self.__service_registry_manager = service_registry_manager
        # instantiate the health and status data object,
        self.__health_status = HealthStatus()
        # instantiate the health and status monitor
        self.__health_status_monitor = HealthStatusMonitor(
                                            self._log_settings,
                                            self._configurations_manager,
                                            self.__service_registry_manager)
        self.__logger.debug("initialized.")

    def __update_last_health_updated(self):
        """"updates the health check times stamp."""
        self.__health_status.last_updated = datetime.now()
        self.__logger.debug(f"last health check updated to "
                            f"{self.__last_health_check()}.")

    def __update_state(self, state):
        """"helper function to update the global state."""
        self.__health_status.current_global_state = state
        self.__logger.info(f'global state is updated to '
                           f'{self.__health_status.current_global_state}')
        self.__logger.info(f'uptime now: {self.uptime()}')

    def current_global_status(self):
        """returns the current global status of the System."""
        return self.__health_status.current_global_status

    def current_global_state(self):
        """returns the current global state of the System."""
        return self.__health_status.current_global_state

    def uptime(self):
        """
        returns the running uptime of the system
        when this method is called.
        """
        return (datetime.now() - self.__health_status.uptime)

    def __last_health_check(self):
        """returns the timestamp when the health and status is last checked."""
        return self.__health_status.last_updated

    def update_global_state(self):
        """"
        It updates the global state if all local states are same.
        It returns an int code indicating whether or not the state is updated.
        """
        all_components, components_with_states =\
            self.__health_status_monitor.validate_local_states()
        # Case all components are UP and in the same state
        if all_components and components_with_states:
            # update the global state as the current local state
            # of the componenets
            self.__update_state(components_with_states[0].current_state)
            # update the last check timestamp
            self.__update_last_health_updated()
            self.__logger.debug("global state is updated.")
            return Response.OK
        else:
            self.__update_state(STATES.ERROR)
            # update the last check timestamp
            self.__update_last_health_updated()
            return Response.ERROR

    def start_monitoring(self):
        """start monitoring thread."""
        self.__health_status_monitor.start_monitoring()

    def finalize_monitoring(self):
        """concludes the monitoring."""
        self.__logger.info("concluding the health and status monitoring.")
        self.__health_status_monitor.finalize_monitoring()
