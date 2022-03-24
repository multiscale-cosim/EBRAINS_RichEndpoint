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
#
# ------------------------------------------------------------------------------
import queue
import signal

from EBRAINS_RichEndpoint.Application_Companion.signal_manager import SignalManager
from EBRAINS_RichEndpoint.Application_Companion.common_enums import EVENT
from EBRAINS_RichEndpoint.Application_Companion.common_enums import Response
from EBRAINS_RichEndpoint.orchestrator.communicator_base import CommunicatorBaseClass


class CommunicatorQueue(CommunicatorBaseClass):
    '''
    Implements the CommunicatorrBaseClass for abstracting
    the underlying communication protocol. This class provides wrappers
    for inter process communication using python Queues.
    '''
    def __init__(self, log_settings, configurations_manager) -> None:
        self._log_settings = log_settings
        self._configurations_manager = configurations_manager
        self.__logger = self._configurations_manager.load_log_configurations(
                                        name=__name__,
                                        log_configurations=self._log_settings)
        self.__signal_manager = SignalManager(
                                        self._log_settings, self._configurations_manager)
        signal.signal(signal.SIGINT,
                      self.__signal_manager.interrupt_signal_handler
                      )
        signal.signal(signal.SIGTERM,
                      self.__signal_manager.kill_signal_handler
                      )
        self.__stop_event = self.__signal_manager.shut_down_event
        self.__kill_event = self.__signal_manager.kill_event
        self.__logger.debug("initialized.")

    def receive(self, endpoint_queue):
        """
        Retrieves the event from the queue.

        Parameters
        ----------
        endpoint_queue : queue
            queue to retrieve the event from

        Returns
        ------
        event from the queue
        """
        while True:
            # wait until an event is retrieved
            # or the process is forcefully quit
            if self.__stop_event.is_set() or self.__kill_event.is_set():
                self.__logger.critical('quitting forcefully!')
                return EVENT.FATAL
            else:
                try:
                    # TODO: Configure the timeout value from XML files
                    current_event = endpoint_queue.get(timeout=10)
                except queue.Empty:
                    self.__logger.debug(f'waiting for the event in {queue}!')
                    continue
                else:
                    return current_event

    def send(self, message, endpoint_queue):
        """sends the message to specified endpoint.

        Parameters
        ----------
        message : ...
            the message to be sent

        endpoint : ...
            destination where the message to bs sent

        Returns
        ------
            return code as int
        """
        self.__logger.debug('sending message.')
        try:
            endpoint_queue.put(message)
            self.__logger.debug('message is sent.')
            return Response.OK
        except queue.Full:
            self.__logger.exception(f'{endpoint_queue} is full.',
                                    exc_info=True)
            return Response.ERROR

    def broadcast_all(self, message, endpoints_queues):
        """broadcasts the message to all specified endpoints.

        Parameters
        ----------

       message : ...
            the message to be sent

        endpoints : ...
            destinations where the message to bs broadcasted

        Returns
        ------
            return code as int
        """
        self.__logger.debug(f'broadcasting {message} '
                            f'to {len(endpoints_queues)}.')
        try:
            for endpoint_queue in endpoints_queues:
                self.__logger.debug(f'sending to {endpoint_queue}')
                endpoint_queue.put(message)
                self.__logger.debug(f'sent {message} to {endpoint_queue}')
            return Response.OK
        except queue.Full:
            self.__logger.exception(f'{endpoint_queue} is full.')
            return Response.ERROR
