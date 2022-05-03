# ------------------------------------------------------------------------------
#  Copyright 2020 Forschungszentrum Jülich GmbH and Aix-Marseille Université
# "Licensed to the Apache Software Foundation (ASF) under one or more contributor
#  license agreements; and to You under the Apache License, Version 2.0. "
#
# Forschungszentrum Jülich
#  Institute: Institute for Advanced Simulation (IAS)
#    Section: Jülich Supercomputing Centre (JSC)
#   Division: High Performance Computing in Neuroscience
# Laboratory: Simulation Laboratory Neuroscience
#       Team: Multi-scale Simulation and Design
#
# ------------------------------------------------------------------------------S
import time

from common.utils import proxy_manager_server_utils
from EBRAINS_RichEndpoint.Application_Companion.common_enums import Response
from EBRAINS_RichEndpoint.Application_Companion.application_companion import ApplicationCompanion
from EBRAINS_RichEndpoint.orchestrator.proxy_manager_server import ProxyManagerServer
from EBRAINS_RichEndpoint.orchestrator.command_control_service import CommandControlService
from EBRAINS_RichEndpoint.orchestrator.orchestrator import Orchestrator
from EBRAINS_RichEndpoint.Application_Companion.common_enums import Response
from EBRAINS_RichEndpoint.Application_Companion.common_enums import SERVICE_COMPONENT_CATEGORY
from EBRAINS_RichEndpoint.steering.poc_steering_menu import POCSteeringMenu
from EBRAINS_RichEndpoint.orchestrator.proxy_manager_client import ProxyManagerClient


class Launcher:
    '''launches the all the necessary components to execute the workflow.'''

    def __init__(self, log_settings, configurations_manager):
        self._log_settings = log_settings
        self._configurations_manager = configurations_manager
        self.__logger = self._configurations_manager.load_log_configurations(
                                        name=__name__,
                                        log_configurations=self._log_settings)
        
        # initialize Proxy Manager Server process
        self.__proxy_manager_server = ProxyManagerServer(
            proxy_manager_server_utils.IP,
            proxy_manager_server_utils.PORT,
            proxy_manager_server_utils.KEY)

        # start the Proxy Manager Server to listen and accept the connection requests
        if self.__proxy_manager_server.start() == Response.ERROR:
            # Case, proxy manager could not be started
            # raise an exception and terminate with error
            proxy_manager_server_utils.terminate_with_error("Launcher could not start Proxy Manager Server!")


        # get client to Proxy Manager Server
        self._proxy_manager_client =  ProxyManagerClient(
            self._log_settings,
            self._configurations_manager)

        # Connect with Proxy Manager Server
        # NOTE: it terminates with RuntimeError if connection could ne be made
        # for whatever reasons
        self._proxy_manager_client.connect(
            proxy_manager_server_utils.IP,
            proxy_manager_server_utils.PORT,
            proxy_manager_server_utils.KEY,
        )

        # Now, get the proxy to registry manager
        self.__component_service_registry_manager =\
             self._proxy_manager_client.get_registry_proxy()

        self.__logger.debug("initialized.")
    
    def __is_component_registered(
                self, componenet_service,
                component_service_name,
                network_delay=2):  # TODO: set with run-time network delay
        '''
        Checks whether the component is registered with registry.
        Returns the component if registered.
        Returns None if not registered.
        '''
        # timout to rule out network delay
        timeout = time.time() + network_delay
        component = None
        while time.time() <= timeout:
            if componenet_service.is_registered_in_registry.is_set():
                component = self.__component_service_registry_manager.\
                    find_all_by_category(component_service_name)
                self.__logger.debug(f'{component} is found.')
                break
            else:
                time.sleep(0.001)  # do not hog CPU
                continue
        if component is None:
            self.__logger.critical(f'TIMEOUT! {componenet_service} '
                                   f'is not registered yet.')
        return component

    def __terminate_application_companions(self, application_companions):
        # terminate the application companions
        for application_companion in application_companions:
            application_companion.terminate()
            application_companion.join()

    def __terminate_command_and_control_service(self, steering_service):
        steering_service.terminate()
        steering_service.join()

    def launch(self, actions):
        '''
        launches the necessary components (Application Companions and
        Orchestrator) as per actions.
        '''
        # 1. append proxy to service registry to action arg list
        # for action in actions:
        #     try:
        #         import base64
        #         import pickle
        #         action['action'].append(base64.b64encode(pickle.dumps(self.__component_service_registry_manager)))
        #     except KeyError:
        #         self.__logger.error(f"No action Popen arg are found"
        #                             f"<{action['action_xml_id']}>")
        
        # 2. initialize Application Comanions
        application_companions = []
        for action in actions:
            application_companions.append(ApplicationCompanion(
                                    self._log_settings,
                                    self._configurations_manager,
                                    action))
        # 3. launch the application companions
        for application_companion in application_companions:
            self.__logger.info('setting up Application Companion.')
            application_companion.start()
        # allow application companions to register with registry
        time.sleep(2)  # TODO: set as run-time network delay

        # 4. initialize steering service
        self.__logger.info('setting up Command and Control service.')
        steering_service = CommandControlService(
                                    self._log_settings,
                                    self._configurations_manager)
        # 5. launch C&S service
        steering_service.start()
        
        # 6. check if C&S is registered
        if self.__is_component_registered(
                    steering_service,
                    SERVICE_COMPONENT_CATEGORY.COMMAND_AND_CONTROL) is None:
            self.__logger.exception('registry returns None for Command and Control \
                                    service.')
            # terminate Application Companions
            self.__terminate_application_companions(application_companions)
            try:
                # raise runtime exception
                raise RuntimeError
            except RuntimeError:
                # log the exception with traceback
                self.__logger.exception('got the exception')
            return Response.ERROR

        # 7. initialize orchestrator
        self.__logger.info('Setting up Orchestrator.')
        orchestrator = Orchestrator(self._log_settings,
                                    self._configurations_manager)
        
        # 8. launch orchestrator
        orchestrator.start()
        orchestrator_component = self.__is_component_registered(
                                    orchestrator,
                                    SERVICE_COMPONENT_CATEGORY.ORCHESTRATOR)
        if orchestrator_component is None:
            # terminate Application Companions
            self.__terminate_application_companions(application_companions)
            # terminate Command and Control service
            self.__terminate_command_and_control_service(steering_service)
            try:
                # raise runtime exception
                raise RuntimeError
            except RuntimeError:
                # log the exception with traceback
                self.__logger.exception('registry returns None for Orchestrator.')
            # terminate with error
            return Response.ERROR

        orchestrator_component_in_queue,  orchestrator_component_out_queue =\
            orchestrator_component[0].endpoint

        # 9. launch the steering menu handler
        # NOTE: this is to demonstrate the POC of steering via CLI
        poc_steering_menu = POCSteeringMenu(self._log_settings,
                                            self._configurations_manager,
                                            orchestrator_component_in_queue,
                                            orchestrator_component_out_queue)
        poc_steering_menu.start_steering()
        return Response.OK
