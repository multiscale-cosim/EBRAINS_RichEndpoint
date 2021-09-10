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
from python.Application_Companion.common_enums import Response


class SignalMonitor:
    '''
    Monitors and acts against the alarm signal that is raised due to global
    state ERROR.
    '''
    def __init__(self, log_settings, configurations_manager,
                 health_status_keeper, alarm_event,
                 frequency=2):  # TODO: set monitoring frequency from XML
        self._log_settings = log_settings
        self._configurations_manager = configurations_manager
        self.__logger = self._configurations_manager.load_log_configurations(
                                        name=__name__,
                                        log_configurations=self._log_settings)
        self.__health_status_keeper = health_status_keeper
        self.__frequency = frequency
        self.__threads_started = []  # keep track of started threads
        self.__alarm_event = alarm_event
        # self.__stop_monitoring = False
        # self.__stop_monitoring = multiprocessing.Event()
        self.__logger.debug("signal monitor is initialized.")

    def __monitor(self):
        '''
        Target function for monitoring thread to keep monitoring if the alarm
        event is set. If it is set, then
        i)   Re-check the global state to rule out network delay,
        iii) Raise signal Interupt if global state is ERROR after recheck,
        iv)  Stop monitoring.
        '''
        while True:
            if self.__alarm_event.is_set():
                self.__logger.critical('re-checking the global state.')
                if self.__health_status_keeper.update_global_state() ==\
                        Response.ERROR:
                    self.__logger.critical('Global state ERROR.')
                    self.__logger.critical('Raising Terminate Signal')
                    signal.raise_signal(signal.SIGINT)
                    self.__logger.critical('stop monitoring.')
                    break
            time.sleep(self.__frequency)
            continue

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
        # self.__stop_monitoring = multiprocessing.Event()
        # keep track of all threads
        self.__threads_started.append(threading.enumerate())
        self.__logger.debug(f"list threads:"
                            f"{self.__threads_started}")
        if(len(self.__threads_started[0]) == threading.active_count()):
            self.__logger.debug('monitoring deamon thread started.')
            return Response.OK
        else:
            return Response.ERROR
