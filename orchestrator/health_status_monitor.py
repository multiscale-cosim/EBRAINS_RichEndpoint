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
from EBRAINS_RichEndpoint.Application_Companion.common_enums import Response


class HealthStatusMonitor:
    """
    1) Monitors the local states and status of each component.
    2) Validates whether all components have same states and their status is UP.
    3) Raises an alarm signal if the states are not the same. 
    """
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
        self.__health_registry_manager_proxy = service_registry_manager
        self.__network_delay = network_delay
        # counter to rule out the network delay before transitioning the
        # global state to ERROR
        self.__counter = 2  # TODO set value from configuration file
        # keep track of started threads
        self.__threads_started = None
        self.__keep_monitoring = None
        self.__logger.debug("initialized.")

    def __is_system_healthy(self, all_components, components_with_states):
        """
        helper function to determine whether the system is healthy i.e.
        1) all components are 'UP' and running
        2) local states of all components are same
        """
        # check if all components are 'UP' and running
        if self.__health_registry_manager_proxy.are_all_statuses_up(all_components):
            # Case a: All components are 'UP' and running
            # now, check if all components have same local states
            return self.__health_registry_manager_proxy.are_all_have_same_state(components_with_states)
        else:
            # Case b: some components are 'DOWN'
            components_with_status_down =\
                self.__health_registry_manager_proxy.components_with_status_down(all_components)
            self.__logger.critical(f"components_with_status_down:"
                                   f" {components_with_status_down}")
            return False

    def __is_global_state_up_to_date(self, components_with_states):
        """checks if the global state is already up-to-date"""
        return bool(self.__health_registry_manager_proxy.current_global_state() == components_with_states[0].current_state)

    def __update_global_state(self):
        """wrapper function to update the global state."""
        return self.__health_registry_manager_proxy.update_global_state()

    def __raise_mayday(self):
        """
        raises interrupt signal so that Orchestrator could terminate the
        execution.
        """
        self.__logger.critical("Global state: 'ERROR'. Raising terminate signal")
        signal.raise_signal(signal.SIGINT)

    @property
    def keep_monitoring(self): return self.__keep_monitoring

    def finalize_monitoring(self):
        # stop monitoring
        self.__keep_monitoring = False
    
    def __monitor_health_status(self):
        '''
        Target function for monitoring thread to monitors the health and
        status of system i.e. the global state is up-to-date, all components
        have the same local state and they are 'UP' and running and no network
        failure is there.

        1) checks whether the local states of all components are the same and
        their statuses are 'UP'. If local states are in-consistent or some
        component is 'DOWN', it raises a terminate signal to indicate
        Orchestrator to terminate the workflow.
        
        2) checks whether the global state is up-to-date. If not it asks
        registry manager to update it as per the transition rules.
        '''
        # set counter to stop rechecking to ruling out the network delay
        counter = self.__counter
        # monitoring loop
        while self.keep_monitoring:
            # fetch all components from registry
            all_components = self.__health_registry_manager_proxy.find_all()
            # filter components without states such as C&C service
            components_with_states =\
                self.__health_registry_manager_proxy.components_with_state(all_components)
            # 1) Check system health i.e. whether all statuses are 'UP' and
            # the local states are the same
            if not self.__is_system_healthy(all_components, components_with_states):
                # NOTE: network delay could delay the state or status update in
                # registry. So, check if network delay is already ruled out.
                if counter == 0:
                    # Case: network delay is ruled out
                    # raise signal so that Orchestrator terminates the workflow
                    self.__raise_mayday()
                    # stop monitoring
                    self.finalize_monitoring()
                    # exit the loop to terminate
                    break

                # Case: network delay is not ruled out yet. So, let the
                # component update its state in registry.
                time.sleep(self.__network_delay)
                # re-check to rule-out the network delay
                self.__logger.critical(f'inconsistent local states. '
                                       f're-checking! counter: {counter}')
                counter = counter - 1
                continue

            # 2) Check whether the global state is not up-to-date yet
            if not self.__is_global_state_up_to_date(components_with_states):
                # update the global state
                self.__logger.debug('updating global state')
                self.__update_global_state()
                self.__logger.info(
                    f'global state is updated to '
                    f'{self.__health_registry_manager_proxy.current_global_state()}')

            # everything is fine i.e. all local states are same, all statuses
            # are 'UP' and global state is up-to-date
            # now, reset the counter to rule out network delay
            counter = self.__counter
            # sleep until next poll
            time.sleep(self.__network_delay)
            # keep monitoring
            continue

        # 3) monitoring is terminated
        self.__logger.info('stopped monitoring.')

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
            self.__logger.debug('monitoring daemon thread started.')
            return Response.OK
        else:
            return Response.ERROR
