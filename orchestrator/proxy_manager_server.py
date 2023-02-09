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
import sys
import pickle
import base64
import threading
from multiprocessing.managers import BaseManager

from EBRAINS_RichEndpoint.registry_state_machine.health_registry_manager import HealthRegistryManager
from EBRAINS_RichEndpoint.application_companion.common_enums import Response


# crete a custom (server process) manager for managing the shared data using
# proxies such as the registry
class Manager(BaseManager):
    pass

# register Health & Registry Manager object
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
        self.__manager = None

    def __start_manager(self):
        try:
            # self.__proxy_manager.start()  # start the manager process
            manager = self.__proxy_manager.get_server()
            manager.serve_forever()
        except EOFError as e:
            # Case a: it finds EOF error while starting the process.
            # So, print the exception and return None to handle it by calling
            # function
            print(e)
            return None

        # Case b, manager process is started
        return self.__proxy_manager
        # return manager

    # def get_manager(self) -> Manager:
    #     '''
    #     starts the manager process
    #     returns the manager object if it is started, otherwise
    #     returns None'''
    #     # instantiate the SyncManager manager object
    #     return Manager(address=(self.__ip, self.__port), authkey=self.__authkey)

    def __server_start(self):
        self.__proxy_manager = Manager(address=(self.__ip, self.__port),
                                       authkey=self.__authkey)
        manager = self.__proxy_manager.get_server()
        print(f'Starting at: {manager.address}')
        stop_timer = threading.Timer(1, lambda:manager.stop_event.set())
        Manager.register('stop', callable=lambda:stop_timer.start())
        manager.serve_forever()
    
    def __register_stop_event_timer(self, server):
        # register the method to start the timer which set the server stop event
        Manager.register('stop_server', callable=lambda:stop_timer.start())
        # timer to set the server stop event
        stop_timer = threading.Timer(1, lambda:server.stop_event.set())

    def start(self):
        '''starts the manager process'''
        # instantiate the manager server object
        self.__proxy_manager = Manager(address=(self.__ip, self.__port),
                                       authkey=self.__authkey)
        # get the Server object whcih is under the control of the Manager
        server = self.__proxy_manager.get_server()
        # register the method to start the timer which set the server stop event
        self.__register_stop_event_timer(server)
        
        # NOTE the launcher looks via Popen PIPEs for the following print
        # statement: "starting Proxy Manager Server at" as a conmfirmation
        # message that the server is started
        print(f"starting Proxy Manager Server at: {server.address}", flush=True)

        # start the server
        server.serve_forever()
        return Response.OK

if __name__ == '__main__':
    if len(sys.argv)==2:
        # TODO better handling of arguments parsing
        
        # 1. unpickle objects
        # unpickle log_settings
        # log_settings = pickle.loads(base64.b64decode(sys.argv[1]))
        # # unpickle configurations_manager object
        # configurations_manager = pickle.loads(base64.b64decode(sys.argv[2]))
        # # unpickle connection detials of Registry Proxy Manager object
        proxy_manager_connection_details = pickle.loads(base64.b64decode(sys.argv[1]))
        # # unpickle range of ports for Orchestrator
        # port_range = pickle.loads(base64.b64decode(sys.argv[4]))
        
        # # 2. security check of pickled objects
        # # it raises an exception, if the integrity is compromised
        # try:
        #     check_integrity(configurations_manager, ConfigurationsManager)
        #     check_integrity(log_settings, dict)
        #     check_integrity(proxy_manager_connection_details, dict)
        #     check_integrity(port_range, dict)
        # except e:
        #     # NOTE an exception is already raised with context when checking the 
        #     # integrity
        #     print(f'pickled object is not an instance of expected type!. {e}')
        #     sys.exit(1)

        # ip = sys.argv[1]
        # port = sys.argv[2]
        # auth_key = sys.argv[3]
        ip = proxy_manager_connection_details["IP"]
        port = proxy_manager_connection_details["PORT"]
        auth_key = proxy_manager_connection_details["KEY"]
        # 3. all is well, instaniate COrchestrator
        proxy_manager_server = ProxyManagerServer(ip, port, auth_key)
        
        # 4. start executing Orchestrator
        if proxy_manager_server.start() == Response.ERROR:
            print('Could not start Server')
            sys.exit(1)    
        else:
            print('Server is started')
            sys.exit(0)
    else:
        print(f'missing argument[s]; required: 2, received: {len(sys.argv)}')
        print(f'Argument list received: {str(sys.argv)}')
        sys.exit(1)
