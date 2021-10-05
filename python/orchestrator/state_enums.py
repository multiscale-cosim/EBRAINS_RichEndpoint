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
class STATES(enum.Enum):
    """ Enum class for states """
    # States belong to Set-up phase at start
    INITIALIZING = 'setup initialization'
    # States belong to Runtime phase
    READY = 'READY'
    SYNCHRONIZING = 'setup synchronization'
    RUNNING = 'running'
    PAUSED = 'paused'
    TERMINATED = 'terminated'
    ERROR = "error"
