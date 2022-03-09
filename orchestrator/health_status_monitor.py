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
import threading
import time
import signal
from EBRAINS_RichEndpoint.Application_Companion.common_enums import SERVICE_COMPONENT_STATUS
from EBRAINS_RichEndpoint.Application_Companion.common_enums import Response


class HealthStatusMonitor:
    def __init__(self, log_settings,
                 configurations_manager,
                 service_registry_manager,
                 network_delay=5  # TODO set at runtime
                 ) -> None:
        self._log_settings = log_settings
        self._configurations_manager = configurations_manager
        self.__logger = self._configurations_manager.load_log_configurations(
                                        name=__name__,
                                        log_configurations=self._log_settings)
        # proxy to registry service
        self.__service_registry_manager = service_registry_manager
        self.__network_delay = network_delay
        # counter to rule out the network delay before transitioning the
        # global state to ERROR
        self.__counter = 2  # TODO set value from configuration file
        # keep track of started threads
        self.__threads_started = None
        self.__keep_monitoring = None
        self.__logger.debug("initialized.")

    def __all_status_up(self, target_components):
        """"
        helper function to check the current local statuses of all components.
        Returns boolean value indicating whether all current local
        statuses are UP.
        """
        return all(component.current_status == SERVICE_COMPONENT_STATUS.UP
                   for component in target_components)

    def __all_have_same_state(self, target_components):
        """"
        helper function to check the current local states of all components.
        Returns boolean value indicating whether all current local
        states are same.
        """
        return all(
            component.current_state == target_components[0].current_state
            for component in target_components)

    def validate_local_states(self):
        '''
        checks the current status and the current state of each component.
        If all have the same current state and status as UP
        then, it returns a tuple of proxies of all components
        and the componenets which have states (e.g. Orchestrator,
        Application Companions).
        Otherwise, it returns a tuple of None.
        '''
        # fetch all components from registry
        all_components = self.__service_registry_manager.find_all()
        components_with_states = []
        # Case, all components are UP
        if self.__all_status_up(all_components):
            self.__logger.debug('all components are UP.')
            # Filter the components which do not have states
            # such as Command and Steering Service
            for component in all_components:
                if component.current_state is not None:
                    components_with_states.append(component)
            self.__logger.debug(f'all componenets: {all_components}; '
                                f'with states: {components_with_states}')
            # check if all components are in same state
            if self.__all_have_same_state(components_with_states):
                return all_components, components_with_states
            else:
                return None, None

        # Case, some component is DOWN
        else:
            # find which components are DOWN
            components_not_running = []
            for component in all_components:
                self.__logger.debug(f'{component.name} status: '
                                    f'{component.current_status}.')
                if component.current_status == SERVICE_COMPONENT_STATUS.DOWN:
                    components_not_running.append(component)
                    return None, None

    @property
    def keep_monitoring(self): return self.__keep_monitoring

    def finalize_monitoring(self):
        # stop monitoring
        self.__keep_monitoring = False

    def __trigger_alarm(self):
        self.__logger.critical('Inconsistent local states. '
                               'Triggering an alarm!')
        signal.alarm(1)  # set alarm signal

    def __monitor_health_status(self):
        '''
        Monitors the current status and the current state of each component.
        Raises an alarm signal, if local states are in-consistent.
        '''
        # set counter to stop rechecking to ruling out the network delay
        counter = self.__counter
        while self.__keep_monitoring:
            # Case a, local states are not the same or all statuses are not UP
            if self.validate_local_states() == (None, None):
                if counter == 0:  # network delay is ruled out
                    # trigger alarm
                    self.__trigger_alarm()
                    # stop monitoring
                    self.finalize_monitoring()
                    # exit to terminate
                    break

                # otherwise, let the component update its state in registry
                time.sleep(self.__network_delay)
                # re-check to rule-out the network delay
                self.__logger.critical(f'inconsistent local states. '
                                       f're-checking counter: {counter}')
                counter = counter - 1
                continue

            # Case b, all local states are same, and statuses are UP.
            # reset counter
            counter = self.__counter
            # sleep until next check
            time.sleep(self.__network_delay)
            # keep monitoring
            continue

        # monitoring is terminated
        self.__logger.critical('stopped monitoring.')

    def start_monitoring(self):
        '''
        Creates a thread to monitor the local states and statuses
        to validate and transition of the global state.
        '''
        # crate a monitoring thread
        health_status_monitor = threading.Thread(
                                name='health and status monitor',
                                target=self.__monitor_health_status)
        # run it in a non-invasive way (in the background)
        # without blocking the health_status_keeper
        health_status_monitor.daemon = True
        # boolean flag to keep monitoring until the globals state is ERROR
        self.__keep_monitoring = True
        health_status_monitor.start()
        # keep track of running threads
        # NOTE this also includes the main thread.
        self.__threads_started = threading.enumerate()
        self.__logger.debug(f"list threads:"
                            f"{self.__threads_started}")
        # test if monitoring threads are running
        if(len(self.__threads_started) == threading.active_count()):
            self.__logger.debug('monitoring deamon thread started.')
            return Response.OK
        else:
            return Response.ERROR
