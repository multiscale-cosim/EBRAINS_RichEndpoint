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
                                        log_configurations=self._log_settings,
                                        directory='logs',
                                        directory_path='AC results')
        self.__logger.debug("logger is configured.")
        # self.__gracefull_shutdown = False
        self.__shut_down_event = multiprocessing.Event()
        self.__kill_event = multiprocessing.Event()
        self.__grace_period = grace_period

    @property
    def kill_event(self): return self.__kill_event

    @property
    def shut_down_event(self): return self.__shut_down_event

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
        if self.__grace_period != 0:
            grace_full_msg = (f'gracefull shutdown in {self.__grace_period}')
        else:
            grace_full_msg = ""  # TODO: proper message for hard exit
        # log the unexpected behavior
        self.__logger.critical(f'Received a stop signal: {grace_full_msg}')
        self.__shut_down_event.set()
