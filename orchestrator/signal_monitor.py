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
import threading
import time
import signal

from EBRAINS_RichEndpoint.Application_Companion.common_enums import Response


class SignalMonitor:
    '''
    Monitors and acts against the alarm signal that is raised due to global
    state ERROR.
    '''
    def __init__(self, log_settings,
                 configurations_manager,
                 alarm_event,
                 monitoring_frequency=5):  # TODO: set monitoring frequency from XML
        self._log_settings = log_settings
        self._configurations_manager = configurations_manager
        self.__logger = self._configurations_manager.load_log_configurations(
                                        name=__name__,
                                        log_configurations=self._log_settings)
        self.__monitoring_frequency = monitoring_frequency
        self.__threads_started = None
        self.__alarm_event = alarm_event
        self.__logger.debug("signal monitor is initialized.")

    def __monitor(self):
        '''
        Target function for monitoring thread to keep monitoring if the alarm
        event is set due to global state ERROR. If it is set, then
        i)   Re-check the global state to rule out network delay,
        iii) Raise signal Interupt if global state is ERROR after recheck,
        iv)  Stop monitoring.
        '''
        while True:
            # Case a, alarm is triggered and captured
            if self.__alarm_event.is_set():
                self.__logger.critical('Global state ERROR.')
                self.__logger.critical('Raising Terminate Signal')
                # raise signal so Orchestrator terminate the workflow
                signal.raise_signal(signal.SIGINT)
                # stop monitoring
                break

            # Case b, alarm  is not yet triggered
            # go to sleep before a re-check
            time.sleep(self.__monitoring_frequency)
            # check if alarm is triggered
            continue

        # signal is raised and montitoring is stopped
        self.__logger.critical('stopped monitoring.')

    def start_monitoring(self):
        '''
        Creates a thread for handling the alarm event which is set when global
        state is ERROR.
        '''
        # crate a monitoring thread
        alarm_signal_monitor = threading.Thread(
                                name='health and status monitor',
                                target=self.__monitor)
        # run it in a non-invasive way (in the background)
        # without blocking the health_status_keeper
        alarm_signal_monitor.daemon = True
        alarm_signal_monitor.start()
        # keep track of all threads
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
