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
from python.orchestrator.service_component import ServiceComponent
from python.orchestrator.service_registry import ServiceRegistry
from python.Application_Companion.common_enums import Response


class ServiceRegistryManager:
    '''
    Manages the registry and discovery of components.
    Provides wrappers to manipulate the current status and active state
    of the components.
    '''
    def __init__(self, log_settings, configurations_manager) -> None:
        self._log_settings = log_settings
        self._configurations_manager = configurations_manager
        self.__logger = self._configurations_manager.load_log_configurations(
                                        name=__name__,
                                        log_configurations=self._log_settings)
        self.__logger.debug("logger is configured.")
        self.__service_registry = ServiceRegistry()

    def __update_component_in_registry(self, component) -> bool:
        """
        helper function for registry update.
        """
        return self.__service_registry.update_component_in_registry(component)

    def register(self, id, name, category, endpoint,
                 current_status, current_state):
        """
        creates and register the data object for service componenet which can
        be shared via proxy.

        Parameters
        ----------
        id : Any
            service component id

        name : Any
            service component name

        category : SERVICE_COMPONENT_CATEGORY
            enum representing service component category

        endpoint : Any
            service component communication endpoint

        current_status : SERVICE_COMPONENT_STATUS
            enum representing current status of the service component

        current_state : STATES
            enum representing current state of the service component

        Returns
        -------
         return code as int
        """
        # initialize the data object
        service_component = ServiceComponent(id, name, category, endpoint,
                                             current_status, current_state)
        # register the data object in registry
        return self.__service_registry.register(service_component)

    # providing this functionality for the sake of completion;
    # not sure if needed. Uncomment if needed.
    # def de_register(self, component):
        # return self.__service_registry.de_register(component)

    def find_by_id(self, component_id) -> ServiceComponent:
        '''wrapper to fetch from registry by id.'''
        return self.__service_registry.find_by_id(component_id)

    def find_by_name(self, component_name) -> ServiceComponent:
        '''wrapper to fetch from registry by name.'''
        return self.__service_registry.find_by_name(component_name)

    def find_all(self) -> list:
        '''wrapper to fetch all from registry.'''
        return self.__service_registry.find_all()

    def find_all_by_category(self, category) -> list:
        '''wrapper to fetch all from registry by given category.'''
        return self.__service_registry.find_all_by_category(category)

    def find_all_by_status(self, status) -> list:
        '''wrapper to fetch all from registry by given status.'''
        return self.__service_registry.find_all_by_status(status)

    def find_all_by_state(self, state) -> list:
        '''wrapper to fetch all from registry by state.'''
        return self.__service_registry.find_all_by_state(state)

    def update_status(self, component, current_status):
        """
        updates the current status of the given component in registry.

        Parameters
        ----------
        component : ServiceComponent
            proxy to service component to be updated.

        current_status: SERVICE_COMPONENT_STATUS
            current status to update

        Returns
        -------
         if updated, proxy to updated component;
         otherwise, an int code representing error.
        """
        component.current_status = current_status
        if self.__update_component_in_registry(component):
            self.__logger.debug(f'{component.name}: status is updated.')
            return self.find_by_id(component.id)
        else:
            self.__logger.error(f'{component.name}: '
                                f'status could not be updated.')
            return Response.ERROR

    def update_state(self, component, current_state):
        """
        updates the current state of the given component in registry.

        Parameters
        ----------
        component : ServiceComponent
            proxy to service component to be updated.

        current_state: STATE
            current status to update

        Returns
        -------
         if updated, proxy to updated component;
         otherwise, an int code representing the error.
        """
        component.current_state = current_state
        if self.__update_component_in_registry(component):
            self.__logger.debug(f'{component.name}: state is updated.')
            return Response.OK
        else:
            self.__logger.error(f'{component.name}: '
                                f'state could not be updated.')
            return Response.ERROR
