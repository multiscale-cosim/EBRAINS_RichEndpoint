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
import signal
import pickle

from EBRAINS_RichEndpoint.application_companion.signal_manager import SignalManager
from EBRAINS_RichEndpoint.application_companion.common_enums import EVENT
from EBRAINS_RichEndpoint.application_companion.common_enums import Response
from EBRAINS_RichEndpoint.orchestrator.communicator_base import CommunicatorBaseClass


class CommunicatorZMQ(CommunicatorBaseClass):
    '''
    Implements the CommunicatorBaseClass for abstracting
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

    def receive(self, zmq_socket, topic=None):
        """
        Retrieves the event from the queue.

        Parameters
        ----------
        zmq_socket : ZmqContext.socket
            the socket bound for receiving

        Returns
        ------
        message received
        """
        message = None
        # wait until a message is received or the process is forcefully
        # quit
        while message is None:
            # check if process is set to forcefully quit
            if self.__stop_event.is_set() or self.__kill_event.is_set():
                self.__logger.critical('quitting forcefully!')
                return EVENT.FATAL
            # Otherwise, wait to receive the message
            try:
                message = zmq_socket.recv_pyobj()
            except Exception:
                # Case, receive time is out
                self.__logger.debug(f'socket: {zmq_socket} waiting for the response!')
                # continue waiting
                continue

        # Message is received
        self.__logger.debug(f'message received: {message}')
        return message

    def send(self, message, zmq_socket):
        """sends the message to specified endpoint.

        Parameters
        ----------
        message : ...
            the message to be sent

        zmq_socket : ZmqContext.socket
            the socket bound for sending

        Returns
        ------
            return code as int
        """
        self.__logger.debug(f'sending {message}')
        try:
            zmq_socket.send_pyobj(message)
            # message is sent
            return Response.OK
        except Exception:
            # message could not be sent
            # log the exception with traceback
            self.__logger.exception(f"message {message} could not be sent")
            return Response.ERROR

    def broadcast_all(self, message, zmq_socket, topic=None):
        """
        broadcasts a given message.

        Parameters
        ----------
        message : ...
            the message to be broadcasted

        zmq_socket : ZmqContext.socket
            the socket bound for publishing

        topic: bytes
            the topic which is associated with the broadcast for filtering
            NOTE the filtering is done on subscriber's side

        Returns
        ------
            return code as int
        """
        self.__logger.debug(f'broadcasting {message}')
        try:
            if topic is not None:
                # send topic in broadcast so the subscriber could filter it
                zmq_socket.send_multipart([topic, pickle.dumps(message)])
            else:
                # just broadcast the message
                self.send(message, zmq_socket)
            # message is sent
            return Response.OK
        except Exception:
            # message could not be broadcasted
            # log the exception with traceback
            self.__logger.exception(f"message {message} could not be broadcasted")
            return Response.ERROR
