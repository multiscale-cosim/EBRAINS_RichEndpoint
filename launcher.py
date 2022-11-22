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
from common.utils import networking_utils
from EBRAINS_RichEndpoint.Application_Companion.common_enums import Response
from EBRAINS_RichEndpoint.Application_Companion.application_companion import ApplicationCompanion
from EBRAINS_RichEndpoint.orchestrator.proxy_manager_server import ProxyManagerServer
from EBRAINS_RichEndpoint.orchestrator.command_control_service import CommandControlService
from EBRAINS_RichEndpoint.orchestrator.orchestrator import Orchestrator
from EBRAINS_RichEndpoint.Application_Companion.common_enums import Response
from EBRAINS_RichEndpoint.Application_Companion.common_enums import SERVICE_COMPONENT_CATEGORY
from EBRAINS_RichEndpoint.steering.steering_service import SteeringService
from EBRAINS_RichEndpoint.orchestrator.proxy_manager_client import ProxyManagerClient


class Launcher:
    '''launches the all the necessary components to execute the workflow.'''

    def __init__(self, log_settings, configurations_manager,
                 proxy_manager_server_address=None,
                 communication_settings_dict=None):
        self._log_settings = log_settings
        self._configurations_manager = configurations_manager

        # to be used for ZMQ (or another communication framework/library)
        self.__communication_settings_dict = communication_settings_dict

        self.__logger = self._configurations_manager.load_log_configurations(
            name=__name__,
            log_configurations=self._log_settings)

        # set Proxy Manager Server connection details
        self.__proxy_manager_connection_details = {}
        self.__set_up_proxy_manager_connection_details(proxy_manager_server_address)

        # initialize Proxy Manager Server process
        self.__proxy_manager_server = ProxyManagerServer(
            self.__proxy_manager_connection_details["IP"],
            self.__proxy_manager_connection_details["PORT"],
            self.__proxy_manager_connection_details["KEY"])

        # start the Proxy Manager Server to listen and accept the connection
        # requests
        if self.__proxy_manager_server.start() == Response.ERROR:
            # Case, proxy manager could not be started
            # raise an exception and terminate with error
            self.__log_exception_and_terminate_with_error(
                "Launcher could not start Proxy Manager Server!")

        # get client to Proxy Manager Server
        self._proxy_manager_client = ProxyManagerClient(
            self._log_settings,
            self._configurations_manager)

        # Connect with Proxy Manager Server
        # NOTE: it terminates with RuntimeError if connection could ne be made
        # for whatever reasons

        self._proxy_manager_client.connect(
            self.__proxy_manager_connection_details["IP"],
            self.__proxy_manager_connection_details["PORT"],
            self.__proxy_manager_connection_details["KEY"])

        # Now, get the proxy to registry manager
        self.__component_service_registry_manager = \
            self._proxy_manager_client.get_registry_proxy()

        # Latency = Propagation Time + Transmission Time + Queuing Time + Processing Delay
        self.__latency = None  # NOTE must be determine runtime

        # setting port range for components
        # print(f'__debug__,__communication_settings_dict={self.__communication_settings_dict}')
        if self.__communication_settings_dict is not None:
            # values were gotten from XML configuration file
            self.__ports_for_command_control_channel = self.__communication_settings_dict
        else:
            # NOTE initializing with hardcoded range from utils
            self.__ports_for_command_control_channel = networking_utils.default_range_of_ports

        # if Command&Control channel is going to be established between multiple nodes
        if self.__ports_for_command_control_channel is not None:
            self.__port_range_for_orchestrator = self.__ports_for_command_control_channel["ORCHESTRATOR"]
            self.__port_range_for_command_control = self.__ports_for_command_control_channel["COMMAND_CONTROL"]
            self.__port_range_for_application_companions = self.__ports_for_command_control_channel[
                "APPLICATION_COMPANION"]
            self.__port_range_for_application_manager = self.__ports_for_command_control_channel[
                "APPLICATION_MANAGER"]

        self.__logger.debug("initialized.")

    def __set_up_proxy_manager_connection_details(
            self, proxy_manager_server_address):
        """
        initializes Proxy Manager Server connection details
        """
        if proxy_manager_server_address is None:
            self.__proxy_manager_connection_details = {
                "IP": proxy_manager_server_utils.IP,
                "PORT": proxy_manager_server_utils.PORT,
                "KEY": proxy_manager_server_utils.KEY}
        else:
            self.__proxy_manager_connection_details = proxy_manager_server_address

    def __get_proxy_to_registered_component(self, component_service_name):
        '''
        It checks whether the component is registered with registry.
        If registered, it returns the proxy to component.
        Otherwise, it returns None.
        '''
        proxy = None
        while not proxy:
            # fetch proxy if component is already registered    
            proxy = self.__component_service_registry_manager.\
                    find_all_by_category(component_service_name)
            if proxy:  # Case, proxy is found
                self.__logger.debug(f'{component_service_name.name} is found '
                                    f'proxy: {proxy}.')
                break
            else:  # Case: proxy is not found yet
                self.__logger.debug(f"{component_service_name.name} is not yet "
                                   "registered, retry in 0.5 second.")
                time.sleep(0.5)  # do not hog CPU
                continue 

        # Case, proxy is found
        return proxy

    def __terminate_application_companions(self, application_companions):
        # terminate the application companions
        for application_companion in application_companions:
            application_companion.terminate()
            application_companion.join()

    def __terminate_launched_component(self, component):
        component.terminate()
        component.join()

    def __log_exception_and_terminate_with_error(self, error_summary):
        """
        Logs the exception with traceback and returns with ERROR as response to
        terminate with error"""
        try:
            # raise RuntimeError exception
            raise RuntimeError
        except RuntimeError:
            # log the exception with traceback
            self.__logger.exception(error_summary)
        # respond with Error to terminate
        return Response.ERROR

    def __compute_latency(self):
        """returns the latency of the network"""
        latency = 10  # TODO determine the latency to registry service
        return latency

    def launch(self, actions):
        '''
        launches the necessary components (Application Companions and
        Orchestrator) as per actions.
        '''
        # determine network delay
        self.__latency = self.__compute_latency()
        
        #TODO remove checks if components are registered -> done by steering_service now
        # new reponsibility for termination of applications? 

        # 1. launch Command&Control service
        self.__logger.info('setting up Command and Control service.')
        cc_service = CommandControlService(
            self._log_settings,
            self._configurations_manager,
            self.__proxy_manager_connection_details,
            self.__port_range_for_command_control)
        cc_service.start()

        # check if Command&Control is already registered with registry
        if self.__get_proxy_to_registered_component(
                SERVICE_COMPONENT_CATEGORY.COMMAND_AND_CONTROL) is None:
            # log exception with traceback and terminate with error
            self.__log_exception_and_terminate_with_error(
                'Command&Control service is not yet registered')

        # 2. launch the Application Companions
        application_companions = []
        for action in actions:
            application_companions.append(ApplicationCompanion(
                self._log_settings,
                self._configurations_manager,
                action,
                self.__proxy_manager_connection_details,
                self.__port_range_for_application_companions,
                self.__port_range_for_application_manager))

        for application_companion in application_companions:
            self.__logger.info('setting up Application Companion.')
            application_companion.start()

        # check if Application Companions are already registered with registry
        for application_companion in application_companions:
            if self.__get_proxy_to_registered_component(
                    SERVICE_COMPONENT_CATEGORY.APPLICATION_COMPANION) is None:
                # terminate Command&Control service
                self.__terminate_launched_component(cc_service)
                # log exception with traceback and terminate with error
                self.__log_exception_and_terminate_with_error(
                    f'{application_companion} is not yet registered')

        # 3. launch Orchestrator
        self.__logger.info('setting up Orchestrator.')
        orchestrator = Orchestrator(self._log_settings,
                                    self._configurations_manager,
                                    self.__proxy_manager_connection_details,
                                    self.__port_range_for_orchestrator)
        orchestrator.start()
        # check if Orchestrator is already registered with registry
        if self.__get_proxy_to_registered_component(
                SERVICE_COMPONENT_CATEGORY.ORCHESTRATOR) is None:
            # terminate Command and Control service
            self.__terminate_launched_component(cc_service)
            # terminate Application Companions
            self.__terminate_application_companions(application_companions)
            # log exception with traceback and terminate with error
            self.__log_exception_and_terminate_with_error(
                'Orchestrator is not yet registered')

        # 4. launch the Steering Service
        # TODO this POC handles both, interactive and non interactive steering
        # -> refactor/rename to represent both -> generic steering
        steering_service = SteeringService(self._log_settings,
                                            self._configurations_manager,
                                            self.__proxy_manager_connection_details,
                                            is_communicate_via_zmqs=True,
                                            is_interactive=False)
        # check if steering service is already registered with registry
        if self.__get_proxy_to_registered_component(
                SERVICE_COMPONENT_CATEGORY.STEERING_SERVICE) is None:
            # terminate Command and Control service
            self.__terminate_launched_component(cc_service)
            # terminate Application Companions
            self.__terminate_application_companions(application_companions)
            # terminate Orchestrator
            self.__terminate_launched_component(orchestrator)
            # log exception with traceback and terminate with error
            self.__log_exception_and_terminate_with_error(
                'Steering Service is not yet registered')
        
        steering_service.start_steering()
        return Response.OK
