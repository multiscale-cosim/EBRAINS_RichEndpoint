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
from python.Application_Companion.common_enums import Response
from python.Application_Companion.application_companion import ApplicationCompanion
from python.orchestrator.routing_manager import RoutingManager
from python.orchestrator.command_steering_service import CommandSteeringService
from python.orchestrator.orchestrator import Orchestrator
from python.Application_Companion.common_enums import Response, SERVICE_COMPONENT_CATEGORY
from python.orchestrator.poc_steering_menu import POCSteeringMenu


class Launcher:
    '''launches the all thge necessary components to execute the workflow.'''
    def __init__(self, log_settings, configurations_manager) -> None:
        self._log_settings = log_settings
        self._configurations_manager = configurations_manager
        self.__logger = self._configurations_manager.load_log_configurations(
                                        name=__name__,
                                        log_configurations=self._log_settings)
        # initialize routing manager for registry and discovery
        self.__routing_manager = RoutingManager.manager()
        self.__component_service_registry_manager =\
            self.__routing_manager.RegistryManager(
                                            self._log_settings,
                                            self._configurations_manager)

        self.__logger.debug("initialized.")

    def __is_component_registered(
                self, componenet_service,
                component_service_name,
                network_delay=0.1):  # TODO: set with run-time network delay
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
                                   f'can not be found in registry.')
        return component

    def launch(self, actions):
        '''
        launches the necessary components (Application Companions and
        Orchestrator) as per actions.
        '''
        application_companions = []
        # i. initialize Application Comanions
        for action in actions:
            application_companions.append(ApplicationCompanion(
                                    self._log_settings,
                                    self._configurations_manager,
                                    action,
                                    self.__component_service_registry_manager))
        # ii. launch the application companions
        for application_companion in application_companions:
            self.__logger.info('setting up Application Companion.')
            application_companion.start()
        # allow application companions to register with registry
        time.sleep(0.5)  # TODO: set as run-time network delay

        # iii. initialize steering service
        self.__logger.info('setting up C&S service.')
        steering_service = CommandSteeringService(
                                    self._log_settings,
                                    self._configurations_manager,
                                    self.__component_service_registry_manager)
        # iv. launch C&S service
        steering_service.start()
        # v. check if C&S is registered
        if self.__is_component_registered(
                    steering_service,
                    SERVICE_COMPONENT_CATEGORY.COMMAND_AND_SERVICE) is None:
            self.__logger.critical('command and steering service is \
                                   not found in registry.')
            return Response.ERROR

        # v. initialize orchestrator
        orchestrator = Orchestrator(self._log_settings,
                                    self._configurations_manager,
                                    self.__component_service_registry_manager)
        # vi. launch orchestrator
        orchestrator.start()

        # vii. launch the steering menu handler
        # NOTE: For now, this is to demonstrate the POC of steering via CLI
        orchestrator_component = self.__is_component_registered(
                                    orchestrator,
                                    SERVICE_COMPONENT_CATEGORY.ORCHESTRATOR)
        if orchestrator_component is None:
            self.__logger.critical('orchestrator is not found in registry.')
            return Response.ERROR
        orchestrator_component_in_queue, _ = orchestrator_component[0].endpoint
        poc_steering_menu = POCSteeringMenu()
        poc_steering_menu.start_menu_handler(orchestrator_component_in_queue)
        return Response.OK
