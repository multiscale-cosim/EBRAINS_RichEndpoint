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
class Response(enum.IntEnum):
    """ Enum class for responses """
    # TODO: extend responses for different types of errors and for each module
    OK = 0
    ERROR = -100
    ERROR_READING_FILE = -170


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
    TRANSFORMER = 4


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
