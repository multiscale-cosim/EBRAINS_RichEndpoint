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
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from typing import Any


@dataclass(init=False)
class HealthStatus:
    """
    Data class for global health and status.
    The attributes are not necessary to be initialized during instantiation.
    """
    # make uptime a private member so it cannot be altered later
    __uptime: datetime = field(default=datetime.now())
    current_global_state: Any
    current_global_status: Any
    last_updated: datetime

    # make a read only public attribute to access the uptime
    @property
    def uptime(self) -> datetime: return self.__uptime
