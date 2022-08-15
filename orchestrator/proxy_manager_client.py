# ------------------------------------------------------------------------------
#  Copyright 2020 Forschungszentrum Jülich GmbH and Aix-Marseille Université
# "Licensed to the Apache Software Foundation (ASF) under one or more contributor
#  license agreements; and to You under the Apache License, Version 2.0. "
# ------------------------------------------------------------------------------
from multiprocessing.managers import BaseManager

from EBRAINS_RichEndpoint.Application_Companion.common_enums import Response


class ProxyManagerClient:
    '''
        Connects to Proxy Manager Server to fetch the proxies to the shared
        data over the network.
    '''

    def __init__(self, log_settings, configurations_manager) -> None:
        self.__log_settings = log_settings
        self.__configurations_manager = configurations_manager
        self.__logger = self.__configurations_manager.load_log_configurations(
                                        name=__name__,
                                        log_configurations=self.__log_settings)
        self.__proxy_manager_client = None
        self.__logger.debug("initialized.")

    def connect(self, ip, port, key):
        '''
        connects with Proxy Manager Server at given IP:Port address using
        the secret Key for authorization.

        Parameters
        ----------
            ip : string
                IP address of the server

            port: int
                port number where server is listening the connection requests

            key: bytes
                authorization key

        Returns
        ------
            response code: int
                response code indicating if the connection is established
        '''
        class Manager(BaseManager): pass
        Manager.register('ServiceRegistryManager')
        self.__proxy_manager_client = Manager(address=(ip, port), authkey=key)
        try:
            self.__proxy_manager_client.connect()
        except ConnectionRefusedError:
            # Case a: connection is refused. print the exception and
            # return Error response to terminate
            self.__logger.exception("Connection is refused!")
            # This is critical, terminate with RunTimeError
            self.__logger.critical(f"Terminating with RuntimeError!")
            # raise RuntimeError exception to terminate
            raise RuntimeError
        
        # Case b: connection is established. Get proxy to registry manager
        self.__logger.debug("Connection is established")
        return Response.OK

    def get_registry_proxy(self):
        '''
        Returns the proxy to registry manager.

        Parameters
        ----------
            ip : string
                IP address of the server

            port: int
                port number where server is listening the connection requests

            key: bytes
                authorization key

        Returns
        ------
            response code: int
                response code indicating if the connection is established
        '''
        return self.__proxy_manager_client.ServiceRegistryManager(
            self.__log_settings,
            self.__configurations_manager)

    def __terminate_with_error(self, exception):
        '''
        Terminates with raising RunTimeError after logging the captured
        exception with traceback.
        '''
        self.__logger.exception(f"EXCEPTION: {exception}.")
        # raise RuntimeError exception to terminate
        self.__logger.critical(f"Terminating with RuntimeError!")
        raise RuntimeError