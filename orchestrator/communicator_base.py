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
import abc


class CommunicatorBaseClass(metaclass=abc.ABCMeta):
    '''
    Abstract base class for data communication.
    It provides wrapper methods for sending and
    receiving data while abstracting the underlying
    communication protocol.
    '''
    @classmethod
    def __subclasshook__(cls, subclass):
        return ((hasattr(subclass, 'send') and
                callable(subclass.send) and
                (hasattr(subclass, 'receive') and
                callable(subclass.receive)) and
                (hasattr(subclass, 'broadcast_all') and
                callable(subclass.broadcast_all))) or
                NotImplemented)

    @abc.abstractmethod
    def send(self, message, endpoint):
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
        raise NotImplementedError

    @abc.abstractmethod
    def receive(self, endpoint):
        """receive the message from specified destination.

        Parameters
        ----------

        endpoint : ...
            from where the message to be received

        Returns
        ------
            message: ...
            return the received meassage
        """
        raise NotImplementedError

    @abc.abstractmethod
    def broadcast_all(self, message, endpoints):
        """broadcast the message to all specified endpoints.

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
        raise NotImplementedError
