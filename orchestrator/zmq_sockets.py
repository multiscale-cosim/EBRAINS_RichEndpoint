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
        # linger period for pending messages
        # NOTE setting default as 0 i.e. to discard immidiately when the socket is
        # closed with zmq_close()
        self.__linger_time = 0 
        self.__logger.debug("initialized")
    
    def create_socket(self, socket_type, receive_timeout=None):
        """
        Creates a socket of the given socket_type and sets its behavior such as
        the maximum number of outstanding messages shall queue in memory for a
        single peer, etc.

        Parameters
        ----------
        socket_type: zmq.Socket
            The zmq socket type to be created
        
        receive_timeout: float
            Maximum time before a recv operation returns with EAGAIN. Default
            value is -1 i.e. it will block until a message is available

        Returns
        -------
        socket: zmq.Socket
            ZMQ socket created as of the given socket_type
        """
        socket = self.__context.socket(socket_type)
        # Set linger period for socket shutdown
        socket.setsockopt(zmq.LINGER, self.__linger_time)
        # set high water mark to remove message dropping for inbound and
        # outbound messages
        socket.setsockopt(zmq.SNDHWM, 0)
        socket.setsockopt(zmq.RCVHWM, 0)
        try:
            socket.setsockopt(zmq.IMMEDIATE, 1)
        except:
            # This parameter was recently added by new libzmq versions
            pass
        # set the maximum time before a recv operation returns with EAGAIN
        if receive_timeout is not None:
            socket.setsockopt(zmq.RCVTIMEO, receive_timeout)
        # accept only routable messages on ROUTER sockets
        if socket_type == zmq.ROUTER:
            sock.setsockopt(zmq.ROUTER_MANDATORY, 1)
        # all is set
        self.__logger.debug(f"created a 0MQ socket: {socket}")
        return socket 
    
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
