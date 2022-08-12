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
from EBRAINS_RichEndpoint.registry_state_machine.service_component import ServiceComponent
from EBRAINS_RichEndpoint.registry_state_machine.service_registry import ServiceRegistry
from EBRAINS_RichEndpoint.registry_state_machine.state_transition_rules import StateTransitionRules
from EBRAINS_RichEndpoint.registry_state_machine.health_status_keeper import HealthStatusKeeper
from EBRAINS_RichEndpoint.registry_state_machine.state_enums import STATES
from EBRAINS_RichEndpoint.registry_state_machine.state_transition_history import LocalStateTransitionRecord
from EBRAINS_RichEndpoint.Application_Companion.common_enums import Response
from EBRAINS_RichEndpoint.Application_Companion.common_enums import SERVICE_COMPONENT_STATUS


class MetaHealthRegistryManager(type):
    """This metaclass ensures there exists only one instance of
    HealthRegistryManager class. It prevents the side-effects such as
    the creation of multiple copies of registry dataclass.
    """
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            # Case: First time instantiation.
            cls._instances[cls] = super(MetaHealthRegistryManager,
                                        cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class HealthRegistryManager(metaclass=MetaHealthRegistryManager):
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
        # instantiate service registry
        self.__service_registry = ServiceRegistry()
        # instantiate state transition manager
        self.__state_transition_rules = StateTransitionRules(self._log_settings,
                                                         self._configurations_manager)
        # instantiate global health and status manager object
        self.__global_health_keeper = HealthStatusKeeper(self._log_settings,
                                                         self._configurations_manager)
        # initialize the global state to 'INITIALIZING'
        self.__global_health_keeper.initialize_global_state(STATES.INITIALIZING)
        # initilaize list to track global state transitions
        self.__global_state_transition_history = [self.current_global_state().name]
        # list to track local state transitions
        self.__local_state_transition_history = []

    def __update_component_in_registry(self, component) -> bool:
        """
        helper function for registry update.
        """
        return self.__service_registry.update_component_in_registry(component)

    def __get_components_with_status_down(self, all_components):
        #  find which components are DOWN
        components_not_running = []
        for component in all_components:
            self.__logger.debug(f'{component.name} status: '
                                f'{component.current_status}.')
            if component.current_status == SERVICE_COMPONENT_STATUS.DOWN:
                components_not_running.append(component)
        return components_not_running

    def __log_exception_with_traceback(self, message):
        try:
            # raise error
            raise RuntimeError
        except RuntimeError:
            # log the exception with traceback
            self.__logger.exception(message)

    def __create_state_transition_record(self, state_before_transition, input_command, state_after_transition):
        return LocalStateTransitionRecord(state_before_transition,
                                          input_command,
                                          state_after_transition)

    def __get_next_legal_state(self, current_state, input_command):
        # get next legal state as per the transition rule
        next_state = self.__state_transition_rules.next_valid_local_state(
            current_state, input_command)
        self.__logger.debug(f"next legal state: {next_state}")
        if next_state == Response.ERROR:
            # log the exception with traceback
            self.__log_exception_with_traceback('Illegal transition rule')
            # return with ERROR to terminate
            return Response.ERROR
        else:
            return next_state

    def register(self, id, name, category, endpoint,
                 current_status, current_state):
        """
        creates and register the data object for service component which can
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

    def are_all_statuses_up(self, target_components):
        """"
        checks the current local statuses of all components.
        Returns boolean value indicating whether all current local
        statuses are 'UP'.
        """
        return all(component.current_status == SERVICE_COMPONENT_STATUS.UP
                   for component in target_components)

    def are_all_have_same_state(self, target_components):
        """"
        helper function to check the current local states of all components.
        Returns boolean value indicating whether all current local
        states are same.
        """
        return all(
            component.current_state == target_components[0].current_state
            for component in target_components)

    def get_components_with_state(self, components):
        components_with_states = []
        for component in components:
            if component.current_state is not None:
                components_with_states.append(component)
        self.__logger.debug(f'all components: {components}; '
                            f'with states: {components_with_states}')
        return components_with_states

    def current_global_status(self):
        """Wrapper tp get the current global status of the System."""
        return self.__global_health_keeper.current_global_status()

    def current_global_state(self):
        """Wrapper to get the current global state of the System."""
        return self.__global_health_keeper.current_global_state()

    def __update_global_state(self, next_valid_global_state):
        """helper function to update the local state to of target component"""
        self.__global_health_keeper.update_global_state(next_valid_global_state)
        # record the transition to history
        self.__global_state_transition_history.append(self.current_global_state().name)
        self.__logger.info(f'current global state state after update: '
                            f'{self.current_global_state()}')
        if next_valid_global_state == STATES.ERROR:
            # log the exception with traceback
            self.__log_exception_with_traceback('Transition Rule is not satisfied')
            return Response.ERROR
        else:
            return Response.OK
    
    def update_global_state(self):
        """
        1) checks whether the constraints are met for transition
        2) follows the transition rule to update the global state
        """
        self.__logger.info(f'current global state before update: '
                           f'{self.current_global_state()}')
        # 1) fetch all components from registry
        all_components = self.find_all()
        components_with_states = self.get_components_with_state(all_components)
        
        # get next valid global state
        next_valid_global_state = self.__state_transition_rules.next_valid_global_state(
            all_components, components_with_states, self.are_all_statuses_up,
            self.are_all_have_same_state)
        
        # check if globals state is already updated by global health monitor
        if self.current_global_state() == next_valid_global_state:
            self.__logger.info("global state is already up-to-date")
            return Response.OK
        
        # 3) otherwise, update the global state
        return self.__update_global_state(next_valid_global_state)
        
    
    # def update_global_state(self):
    #     """
    #     1) checks whether the constraints are met for transition
    #     2) follows the transition rule to update the global state
    #     """
    #     # NOTE: Transition rule to update global state
    #     #   RULE: Global state is the same as the local state of a given
    #     #         component
    #     #   CONSTRAINT: 1) all local statuses must be 'UP'
    #     #               2) all local states must be the same
    #     #   ERROR: if the constraint is not met then the global state is ERROR
    #     self.__logger.debug(f'current global state before update: '
    #                         f'{self.__global_health_keeper.current_global_state()}')
    #     # 1) fetch all components from registry
    #     all_components = self.find_all()
        
    #     # 2) check whether the constrains are met
    #     # 2.1) check if all local statuses are 'UP'
    #     # Case 1, some component is DOWN
    #     if not self.are_all_statuses_up:
    #         # update global state to ERROR
    #         self.__global_health_keeper.update_global_state(STATES.ERROR)
    #         # get which components are 'DOWN'
    #         self.__logger.critical(f'components are DOWN: '
    #                                f'{self.__get_components_with_status_down(all_components)}')
    #         # log the exception with traceback and return with ERROR to terminate
    #         self.__log_exception_with_traceback('global state can not be updated')
    #         # return with ERROR to terminate
    #         return Response.ERROR

    #     # Case 2: all components are 'UP', now update the global state
    #     # as per transition rule
    #     self.__logger.debug("all components are 'UP'")

    #     # filter components without states such as C&C service
    #     components_with_states = self.get_components_with_state(all_components)
        
    #     # check if globals state is already updated by global health monitor
    #     if self.current_global_state() == components_with_states[0].current_state:
    #         self.__logger.debug("global state is already up-to-date")
    #         self.__global_state_transition_history.append(self.current_global_state().name)
    #         return Response.OK

    #     # 2.2) Now, check if all local states are same
    #     # Case 1: components have different states
    #     if not self.are_all_have_same_state(components_with_states):
    #         # update global state to ERROR
    #         self.__global_health_keeper.update_global_state(STATES.ERROR)
    #         # log the exception with traceback
    #         self.__log_exception_with_traceback('components have different states.'
    #                                             ' global state can not be updated')
    #         # return with ERROR to terminate
    #         return Response.ERROR

    #     # Case 2: all components have same state

    #     self.__logger.debug(f'current state of all components: '    
    #                        f'{components_with_states[0].current_state}')

    #     # 3) Both constraints are satisfied. Now, follow the transition rule
    #     # to update the global state to the local state of any component
    #     self.__global_health_keeper.update_global_state(
    #         components_with_states[0].current_state)
    #     self.__global_state_transition_history.append(self.current_global_state())
    #     self.__logger.debug(f'current global state state after update: '
    #                         f'{self.__global_health_keeper.current_global_state()}')
    #     return Response.OK

    def update_state_transition_history(self, state_before_transition,
                                        input_command, state_after_transition):
        # create a state transition record
        self.__local_state_transition_record = self.__create_state_transition_record(
            state_before_transition, input_command, state_after_transition)
        self.__local_state_transition_history.append(self.__local_state_transition_record)

    def get_local_state_transition_history(self):
        return self.__local_state_transition_history

    def get_global_state_transition_history(self):
        return self.__global_state_transition_history
    
    def __update_local_state(self, target_component, next_legal_state):
        """helper function to update the local state to of target component"""
        target_component.current_state = next_legal_state
        if self.__update_component_in_registry(target_component):
            self.__logger.debug(f'{target_component.name} local state is updated to: '
                                f'{target_component.current_state}')
            # state is updated, return proxy to up-to-date component
            return target_component
        else:
            # log exception with traceback
            self.__log_exception_with_traceback(f'{target_component.name}: '
                                                'is not found in registry.')
            # return with error to terminate
            return Response.ERROR

    def update_local_state(self, component, input_command):
        """
        updates the current state of the given component in registry.

        Parameters
        ----------
        component : ServiceComponent
            proxy to service component whose local state is to be updated

        current_state: STATE.Enum
            current local state

        input_command: SteeringCommands.Enum
            the command to transit from the current state to next legal state

        Returns
        -------
            returns an int code representing whether the state is updated to
            next legal state or not
        """
        current_state = component.current_state
        self.__logger.debug(f'current local state: {current_state}')
        self.__logger.debug(f'input command: {input_command}')
        # validate transition rule to get the next legal state
        next_legal_state = self.__get_next_legal_state(
            current_state, input_command)

        # Case a: transition rule is not valid and local state is ERROR
        if next_legal_state == STATES.ERROR:
            self.__update_local_state(component, next_legal_state)
            # log exception with traceback
            self.__log_exception_with_traceback("invalid transition rule."
                                                " local state is transited to:"
                                                f" {next_legal_state.name}")
            # return with ERROR to terminate
            return Response.ERROR

        # Case b: transition rule is valid
        # update component's local state to next legal state
        return self.__update_local_state(component, next_legal_state)
