# ------------------------------------------------------------------------------
#  Copyright 2020 Forschungszentrum Jülich GmbH and Aix-Marseille Université
# "Licensed to the Apache Software Foundation (ASF) under one or more
#  contributor license agreements; and to You under the Apache License,
#  Version 2.0. "
#
# Forschungszentrum Jülich
#  Institute: Institute for Advanced Simulation (IAS)
#    Section: Jülich Supercomputing Centre (JSC)
#   Division: High Performance Computing in Neuroscience
# Laboratory: Simulation Laboratory Neuroscience
#       Team: Multi-scale Simulation and Design
# ------------------------------------------------------------------------------
import multiprocessing
import time


class SignalManager:
    """
    Facilitates to handle the OS signals such as SIGINT, etc.
    """
    def __init__(self, log_settings,
                 configurations_manager,
                 grace_period=0
                 ):
        self._log_settings = log_settings
        self._configurations_manager = configurations_manager
        self.__logger = self._configurations_manager.load_log_configurations(
                                        name=__name__,
                                        log_configurations=self._log_settings)
        self.__logger.debug("logger is configured.")
        # self.__gracefull_shutdown = False
        self.__shut_down_event = multiprocessing.Event()
        self.__kill_event = multiprocessing.Event()
        self.__alarm_event = multiprocessing.Event()
        self.__grace_period = grace_period

    @property
    def kill_event(self): return self.__kill_event

    @property
    def shut_down_event(self): return self.__shut_down_event

    @property
    def alarm_event(self): return self.__alarm_event

    def reset_alarm(self): self.__alarm_event.clear()

    def kill_signal_handler(self, *args):
        """
        handler for SIGTERM signal
        """
        # log the unexpected behavior
        self.__logger.critical("Received a direct kill signal, shutting down")
        self.__kill_event.set()

    def interrupt_signal_handler(self, *args):
        """
        handler for SIGINT signal
        """
        self.__shut_down_event.set()
        # self.__gracefull_shutdown = True

        # We give possible external running application some time to shutdown
        if self.__grace_period > 0:
            grace_full_msg = (f'gracefull shutdown in {self.__grace_period}')
            self.__logger.debug(f'sleeping for grace period: '
                                f'{self.__grace_period} sec')
            time.sleep(self.__grace_period)
        else:
            grace_full_msg = "Quitting forcefully without a grace period!"
        # log the unexpected behavior
        self.__logger.critical(f'Received a stop signal: {grace_full_msg}')
        self.__shut_down_event.set()

    def alarm_signal_handler(self, *args):
        """
        handler for SIGALRM signal
        """
        # log the unexpected behavior
        self.__logger.critical("Received an alarm signal.")
        self.__alarm_event.set()
        self.__logger.critical("Alarm event is set.")
