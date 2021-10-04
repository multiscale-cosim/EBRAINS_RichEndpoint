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
from typing import Any
from python.Application_Companion.common_enums import SERVICE_COMPONENT_CATEGORY
from python.Application_Companion.common_enums import SERVICE_COMPONENT_STATUS
from python.orchestrator.state_enums import STATES


@dataclass  # dataclass to avoid the boilerplate pain
class ServiceComponent:
    """
    Data class for the service components to facilitate their discovery
    and communication. This is particularly useful if later it needs to be
    stored to a more persistant location.
    """
    # private attribute that can only be set at initialization
    __id: Any  # process id
    # private attribute that can only be set at initialization
    __name: Any  # process name
    # private attribute that can only be set at initialization
    __category: SERVICE_COMPONENT_CATEGORY  # process category
    # private attribute that can only be set at initialization
    __endpoint: Any  # communication endpoint
    # public attribute that can be updated later
    current_status: SERVICE_COMPONENT_STATUS  # current local status
    # public attribute that can be updated later
    current_state: STATES  # current local state

    # make a read only public attribute to access the id
    @property
    def id(self) -> Any:
        return self.__id

    # make a read only public attribute to access the name
    @property
    def name(self) -> Any:
        return self.__name

    # make a read only public attribute to access the category
    @property
    def category(self) -> Any:
        return self.__category

    # make a read only public attribute to access the endpoint
    @property
    def endpoint(self) -> Any:
        return self.__endpoint