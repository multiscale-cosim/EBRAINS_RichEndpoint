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


class DBManagerBaseClass(metaclass=abc.ABCMeta):
    '''
    Abstract base class for handling the data.
    NOTE: Later, the functionality to be extended for all CRUD commands
    to support the data loading and manipulation.
    '''
    @classmethod
    def __subclasshook__(cls, subclass):
        return (hasattr(subclass, 'write') and
                callable(subclass.write) or
                NotImplemented)

    @abc.abstractmethod
    def write(self, database: str, data: dict):
        """stores the data to specified database.

        Parameters
        ----------
        database : str
            the location to store the data

        data : dict
            the data to be stored

        Returns
        ------
            return code as int
        """
        raise NotImplementedError
