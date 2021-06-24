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
class SteeringCommands(enum.Enum):
    """ Enum class for steering Commands """
    INIT = 'init'
    START = 'start'
    END = 'end'


@enum.unique
class Response(enum.Enum):
    """ Enum class for responses """
    # TODO: extend responses for different types of errors and for each module
    OK = 0
    ERROR = -1
