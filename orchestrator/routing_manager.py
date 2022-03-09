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
from multiprocessing.managers import BaseManager

from EBRAINS_RichEndpoint.orchestrator.service_registry_manager import ServiceRegistryManager


# crete a custom (server process) manager for manipulating the registry
# via proxies
class Manager(BaseManager):
    pass


# register ServiceRegistryManager class
# all of its public methods are accessible via proxy
Manager.register('get_registry_manager', ServiceRegistryManager)


class RoutingManager:
    @classmethod
    def manager(cls) -> Manager:
        routing_manager = Manager()
        routing_manager.start()  # start the manager
        return routing_manager
