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
from EBRAINS_RichEndpoint.Application_Companion.common_enums import Response


class ServiceRegistry:
    '''
    Registry for the service components. It offers the functionality for
    registry and discovery of the service components.
    '''
    def __init__(self) -> None:
        self.__registry = []

    def register(self, component):
        """
        register the data object for service componenet.

        Parameters
        ----------
        component : ServiceComponent
          data object of service component to be registered.

        Returns
        -------
         return code as int
        """
        if(self.__registry.append(component) is None):
            return Response.OK
        else:
            return Response.ERROR

    # providing this functionality for the sake of completion;
    # not sure if we need this. Uncomment if needed.
    # def de_register(self, component):
    #     if(component in self.__registry):
    #         self.__registry.remove(component)
    #         return Response.OK
    #     else:
    #         return Response.ERROR

    def find_by_id(self, component_id):
        '''
        fetches the proxy to service component by id

        Parameters
        ----------
        component_id : ...
            id of service component

        Returns
        -------
        proxy to found component;
        returns None if not found.
        '''
        for component in self.__registry:
            if component.id == component_id:
                # Case a: component is found
                return component

        # Case b: component is not found
        return None

    def find_by_name(self, component_name):
        '''
        fetches the proxy to service component by name

        Parameters
        ----------
        component_name : ...
            name of service component

        Returns
        -------
        proxy to found component;
        returns None if not found.
        '''
        for component in self.__registry:
            if component.name == component_name:
                # Case a: component is found
                return component
        
        # Case b: component is not found
        return None

    def find_all(self):
        '''fetches all from registry.'''
        return self.__registry

    def find_all_by_category(self, category):
        '''fetches all from registry by given category.'''
        components = [component for component in self.__registry
                      if component.category == category]
        return components

    def find_all_by_status(self, status):
        '''fetches all from registry by given status.'''
        components = [component for component in self.__registry
                      if component.current_status == status]
        return components

    def find_all_by_state(self, state):
        '''fetches all from registry by given state.'''
        components = [component for component in self.__registry
                      if component.current_state == state]
        return components

    def update_component_in_registry(self, component) -> bool:
        '''
        updates the given component in registry.
        returns boolean value indicating whether or not registry is updated.
        '''
        is_updated = False
        for index, old_component in enumerate(self.__registry):
            if old_component.id == component.id:
                self.__registry[index] = component
                is_updated = True
        return is_updated
