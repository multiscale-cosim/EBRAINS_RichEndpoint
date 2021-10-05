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
from python.orchestrator.command_control_service import CommandControlService
from python.orchestrator.orchestrator import Orchestrator
from python.Application_Companion.common_enums import Response
from python.Application_Companion.common_enums import SERVICE_COMPONENT_CATEGORY
from python.orchestrator.poc_steering_menu import POCSteeringMenu


class Launcher:
    '''launches the all the necessary components to execute the workflow.'''

    def __init__(self, log_settings, configurations_manager) -> None:
        self._log_settings = log_settings
        self._configurations_manager = configurations_manager
        self.__logger = self._configurations_manager.load_log_configurations(
                                        name=__name__,
                                        log_configurations=self._log_settings)
        # initialize routing manager for registry and discovery
        self.__routing_manager = RoutingManager.manager()
        self.__component_service_registry_manager =\
            self.__routing_manager.get_registry_manager(
                                            self._log_settings,
                                            self._configurations_manager)

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
        time.sleep(2)  # TODO: set as run-time network delay

        # iii. initialize steering service
        self.__logger.info('setting up Command and Control service.')
        steering_service = CommandControlService(
                                    self._log_settings,
                                    self._configurations_manager,
                                    self.__component_service_registry_manager)
        # iv. launch C&S service
        steering_service.start()
        # v. check if C&S is registered
        if self.__is_component_registered(
                    steering_service,
                    SERVICE_COMPONENT_CATEGORY.COMMAND_AND_SERVICE) is None:
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

        # v. initialize orchestrator
        self.__logger.info('Setting up Orchestrator.')
        orchestrator = Orchestrator(self._log_settings,
                                    self._configurations_manager,
                                    self.__component_service_registry_manager)
        # vi. launch orchestrator
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

        orchestrator_component_in_queue, _ = orchestrator_component[0].endpoint
        # vii. launch the steering menu handler
        # NOTE: this is to demonstrate the POC of steering via CLI
        poc_steering_menu = POCSteeringMenu()
        poc_steering_menu.start_menu_handler(orchestrator_component_in_queue)
        return Response.OK
