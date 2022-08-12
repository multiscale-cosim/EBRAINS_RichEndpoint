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

from EBRAINS_RichEndpoint.registry_state_machine.health_registry_manager import HealthRegistryManager
from EBRAINS_RichEndpoint.Application_Companion.common_enums import Response


# crete a custom (server process) manager for managing the shared data using
# proxies such as the registry
class Manager(BaseManager):
    pass


# register method to get the proxy
Manager.register('ServiceRegistryManager', HealthRegistryManager)


class ProxyManagerServer:
    '''
        starts the Custom Manager Process to share the data between the
        processes over the network.
    '''
    def __init__(self, ip, port, authkey):
        self.__ip = ip
        self.__port = port
        self.__authkey = authkey
        self.__proxy_manager = None

    def __start_manager(self):
        try:
            self.__proxy_manager.start()  # start the manager process
        except EOFError as e:
            # Case a: it finds EOF error while starting the process.
            # So, print the exception and retrun None to handle it by calling
            # function
            print(e)
            return None

        # Case b, manager process is started
        return self.__proxy_manager

    def get_manager(self) -> Manager:
        '''
        starts the manager process
        returns the manager object if it is started, otherwise
        returns None'''
        # instantiate the SyncManager manager object
        return Manager(address=(self.__ip, self.__port), authkey=self.__authkey)

    def start(self):
        '''starts the manager process'''
        # instantiate the manager server object
        self.__proxy_manager = self.get_manager()
        if self.__start_manager() is None:
            # Case a: manager process could not be started
            # NOTE: exception is already printed with context at its origin
            return Response.ERROR
        
        # Case b: manager process is started
        return Response.OK
