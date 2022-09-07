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
import zmq

from EBRAINS_RichEndpoint.Application_Companion.common_enums import Response


class ZMQSockets:
    """
    Wrapper class to provide the methods to
    1. create ZMQ sockets,
    2. set ZMQ socket options
    3. bind a socket to first available port in a given range
    """
    def __init__(self, log_settings, configurations_manager) -> None:
        self._log_settings = log_settings
        self._configurations_manager = configurations_manager
        self.__logger = self._configurations_manager.load_log_configurations(
                                        name=__name__,
                                        log_configurations=self._log_settings)
        self.__context = zmq.Context()
        self.__logger.debug("initialized")

    def req_socket(self, receive_timeout=None):
        """creates and returns REQ socket type"""
        socket = self.__context.socket(zmq.REQ)
        if receive_timeout is not None:
            socket.setsockopt(zmq.RCVTIMEO, receive_timeout)
        return socket

    def rep_socket(self):
        """creates and returns REP socket type"""
        return self.__context.socket(zmq.REP)

    def push_socket(self):
        """creates and returns PUSH socket type"""
        return self.__context.socket(zmq.PUSH)

    def pull_socket(self):
        """creates and returns PUSH socket type"""
        return self.__context.socket(zmq.PULL)

    def pub_socket(self):
        """creates and returns PUB socket type"""
        return self.__context.socket(zmq.PUB)

    def sub_socket(self):
        """creates and returns PUB socket type"""
        return self.__context.socket(zmq.SUB)

    def subscribe_to_topic(self, sub_socket, subscription_topic):
        """
        subscribes to a given topic to filter the messages on subscriber side
        """
        sub_socket.setsockopt(zmq.SUBSCRIBE, subscription_topic)

    def bind_to_first_available_port(self, zmq_socket, ip, min_port, max_port,
                                     max_tries=100):
        """
        It binds the socket to the first available port in a range.
        It returns the port the socket is bound to.

        Parameters
        ----------
        socket: zmq.Socket
            The zmq socket to be bound to the first available port in the range
        ip : str
            IP address (without port) where to bound the socket

        min_port: int
            The minimum port in the range of ports to try (inclusive)

        max_port: int
            The maximum port in the range of ports to try (exclusive)

        max_tries: int
            The maximum number of attempts to make for binding

        Returns
        -------
        port: int
            The port the socket is bound to

        It returns the int code representing the error while binding for any
        reason such as max tries are reached before a successful binding.
        """
        try:
            port = zmq_socket.bind_to_random_port(
                    f"tcp://{ip}",
                    min_port=min_port,
                    max_port=max_port,
                    max_tries=max_tries)
            self.__logger.debug(f"socket {zmq_socket} is bound to {ip}:{port}")
            return port
        except Exception:  # e.g. no port is free in the provided range
            # log exception with traceback
            self.__logger.exception("Could not bound in range "
                                    f"min_port:{min_port}, "
                                    f"max_port:{max_port}, "
                                    f"max_tries:{max_tries}")
            return Response.ERROR
