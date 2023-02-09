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
import enum


@enum.unique
class SteeringCommands(enum.IntEnum):
    """ Enum class for steering Commands """
    INIT = 1
    START = 2
    END = 3
    EXIT = 4
    PAUSE = 5  # TODO: add support
    RESUME = 6  # TODO: add support


@enum.unique
class COMMANDS(enum.IntEnum):
    """ Enum class for commands """
    STEERING_COMMAND = 0
    PARAMETERS = 1

@enum.unique
class Response(enum.IntEnum):
    """ Enum class for responses """
    # TODO: extend responses for different types of errors and for each module
    OK = 0
    ERROR = -100
    ERROR_READING_FILE = -170
    NOT_VALID_COMMAND = -200


@enum.unique
class EVENT(enum.IntEnum):
    """ Enum class for responses """
    # TODO: extend to support different event types
    FATAL = -100
    STATE_UPDATE_FATAL = -101


@enum.unique
class SERVICE_COMPONENT_CATEGORY(enum.IntEnum):
    """ Enum class for services (components) """
    APPLICATION_COMPANION = 0
    ORCHESTRATOR = 1
    COMMAND_AND_CONTROL = 2
    STEERING_SERVICE = 3
    INTERSCALE_HUB = 4
    APPLICATION_MANAGER = 5
    PROXY_MANAGER_SERVER = 6  # manages proxies e.g. registry service proxy


@enum.unique
class SERVICE_COMPONENT_STATUS(enum.IntEnum):
    """ Enum class for services (components) status"""
    UP = 0
    DOWN = 1


@enum.unique
class INTEGRATED_SIMULATOR_APPLICATION(enum.IntEnum):
    """ Enum class for integrated applications (simulators)"""
    PID = 0
    LOCAL_MINIMUM_STEP_SIZE = 1


@enum.unique
class INTEGRATED_INTERSCALEHUB_APPLICATION(enum.IntEnum):
    """ Enum class for integrated applications (InterscaleHub)"""
    PID = 0
    MPI_CONNECTION_INFO = 1
    INTERCOMM_TYPE = 2  # sender or receiver
    DATA_EXCHANGE_DIRECTION = 3  # NEST_to_TVB or TVB_to_NEST


@enum.unique
class INTERCOMM_TYPE(enum.IntEnum):
    """ Enum class for the type of Intercomm"""
    RECEIVER = 0
    SENDER = 1


@enum.unique
class PUBLISHING_TOPIC(enum.Enum):
    """
    Enum class for publishing topics (a set of) subscribers can subscribe to
    """
    # NOTE ZMQ set socket options i.e. setsockopt() requires the topic to be
    # in bytes
    STEERING = b'steering'


@enum.unique
class MONITOR(enum.Enum):
    """ Enum class for integrated applications (simulators)"""
    PID_PROCESS_BEING_MONITORED = 0  # PID of the process being monitored
    RESOURCE_USAGE_MONITOR = 1
