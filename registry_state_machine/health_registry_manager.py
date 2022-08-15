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
from EBRAINS_RichEndpoint.registry_state_machine.state_transition_validator import StateTransitionValidator
from EBRAINS_RichEndpoint.registry_state_machine.health_status_keeper import HealthStatusKeeper
from EBRAINS_RichEndpoint.registry_state_machine.state_enums import STATES
from EBRAINS_RichEndpoint.registry_state_machine.state_transition_record import LocalStateTransitionRecord
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
    1) Manages the registry and discovery of components.
    2) Manages the health and status of the individual components and th
    overall system.
    '''
    def __init__(self, log_settings, configurations_manager) -> None:
        self._log_settings = log_settings
        self._configurations_manager = configurations_manager
        self.__logger = self._configurations_manager.load_log_configurations(
                                        name=__name__,
                                        log_configurations=self._log_settings)
        # instantiate service registry
        self.__service_registry = ServiceRegistry()
        # instantiate state transition manager
        self.__state_transition_validator = StateTransitionValidator(
                                                self._log_settings,
                                                self._configurations_manager)
        # instantiate global health and status manager object
        self.__global_health_keeper = HealthStatusKeeper(self._log_settings,
                                                         self._configurations_manager)
        # initialize the global state to 'INITIALIZING'
        self.__global_health_keeper.initialize_global_state(STATES.INITIALIZING)
        # initialize list to track global state transitions
        self.__global_state_transition_history = [self.current_global_state().name]
        # list to track local state transitions
        self.__local_state_transition_history = []
        self.__logger.debug("Initialized.")

    def __update_component_in_registry(self, component) -> bool:
        """
        helper function to update the given component in registry.
        """
        return self.__service_registry.update_component_in_registry(component)

    def __create_local_state_transition_record(self,
                                               state_before_transition,
                                               input_command,
                                               state_after_transition):
        """helper function to create the local state transition record"""
        return LocalStateTransitionRecord(state_before_transition,
                                          input_command,
                                          state_after_transition)

    def __next_valid_local_state(self, current_state, input_command):
        """
        helper function to get the next valid local state as per transition
        rules.

        Parameters
        ----------
        current_state: STATES.Enum
            current state

        input_command : SteeringCommands.Enum
            The input to transit from current state to next valid state

        Returns
        ------
        next_valid_state: STATES.Enum
            returns next valid state if the input provided is valid transit
            from current state

        ERROR: RESPONSE.Enum
            returns Error if the input provided is not valid to transit from
            current state
        """
        # get next legal state as per the transition rule
        next_state = self.__state_transition_validator.next_valid_local_state(
            current_state, input_command)
        self.__logger.debug(f"next legal state: {next_state}")
        if next_state == Response.ERROR:
            # log the exception with traceback
            self.__log_exception_with_traceback('Illegal transition rule')
            # return with ERROR to terminate
            return Response.ERROR
        else:
            return next_state

    def __update_global_state(self, next_valid_global_state):
        """helper function to transit the global state to next valid state."""
        self.__global_health_keeper.update_global_state(
            next_valid_global_state)
        # record the transition to history
        self.__global_state_transition_history.append(
            self.current_global_state().name)
        self.__logger.debug(f'current global state state after update: '
                            f'{self.current_global_state()}')
        if next_valid_global_state == STATES.ERROR:
            # log the exception with traceback
            self.__log_exception_with_traceback('Transition Rule is not satisfied')
            return Response.ERROR
        else:
            return Response.OK

    def __update_local_state(self, target_component, next_valid_state):
        """helper function to update the local state of target component"""
        target_component.current_state = next_valid_state
        if self.__update_component_in_registry(target_component):
            self.__logger.debug(f'{target_component.name} local state is updated '
                                f'to: {target_component.current_state}')
            # state is updated, return proxy to up-to-date component
            return target_component
        else:
            # log exception with traceback
            self.__log_exception_with_traceback(f'{target_component.name}: '
                                                'is not found in registry.')
            # return with error to terminate
            return Response.ERROR

    def __log_exception_with_traceback(self, message):
        """
        helper function to log the exception with traceback and user
        provided message"""
        try:
            # raise RunTimeError exception
            raise RuntimeError
        except RuntimeError:
            # log the exception with traceback
            self.__logger.exception(message)

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

    # NOTE: This functionality is provided only for the sake of completion.
    # Uncomment it if the functionality is needed.
    # def de_register(self, component):
        # return self.__service_registry.de_register(component)

    def find_by_id(self, component_id) -> ServiceComponent:
        '''wrapper to fetch component from registry by id.'''
        return self.__service_registry.find_by_id(component_id)

    def find_by_name(self, component_name) -> ServiceComponent:
        '''wrapper to fetch component from registry by name.'''
        return self.__service_registry.find_by_name(component_name)

    def find_all(self) -> list:
        '''wrapper to fetch all components from registry.'''
        return self.__service_registry.find_all()

    def find_all_by_category(self, category) -> list:
        '''wrapper to fetch all components from registry by given category.'''
        return self.__service_registry.find_all_by_category(category)

    def find_all_by_status(self, status) -> list:
        '''wrapper to fetch all components from registry by given status.'''
        return self.__service_registry.find_all_by_status(status)

    def find_all_by_state(self, state) -> list:
        '''wrapper to fetch all components from registry by state.'''
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

    def are_all_statuses_up(self, target_components) -> bool:
        """"
        checks the current local statuses of all components.
        Returns boolean value indicating whether all current local
        statuses are 'UP'.
        """
        return all(component.current_status == SERVICE_COMPONENT_STATUS.UP
                   for component in target_components)

    def are_all_have_same_state(self, tar_components) -> bool:
        """"
        checks whether the current local states of all components is same.
        Returns boolean value indicating whether all current local states are
        same.
        """
        return all(
            component.current_state == tar_components[0].current_state
            for component in tar_components)

    def components_with_state(self, all_components):
        """
        Filters the components without states such as C&C service, and returns
        only the components which have states.

        Parameters
        ----------
        all_components : ServiceComponent
            the list of components to be filtered by state

        current_status: SERVICE_COMPONENT_STATUS
            current status to update

        Returns
        -------
        components_with_state: list
            list of components which have states e.g. Application Companions
        """
        components_with_states = []
        for component in all_components:
            if component.current_state is not None:
                components_with_states.append(component)
        self.__logger.debug(f'all components: {all_components}; '
                            f'with states: {components_with_states}')
        return components_with_states

    def current_global_status(self):
        """Wrapper to get the current global status of the system."""
        return self.__global_health_keeper.current_global_status()

    def current_global_state(self):
        """Wrapper to get the current global state of the system."""
        return self.__global_health_keeper.current_global_state()

    def local_state_transition_history(self):
        """wrapper function to fetch local state transition history"""
        return self.__local_state_transition_history

    def global_state_transition_history(self):
        """wrapper function to fetch global state transition history"""
        return self.__global_state_transition_history

    def system_up_time(self):
        """wrapper function to fetch the up time of system since the start."""
        uptime_till_now = self.__global_health_keeper.uptime_till_now()
        self.__logger.debug(f"up time till now: {uptime_till_now}")
        return uptime_till_now     

    def update_global_state(self):
        """
        1) Checks whether the constraints are met for global state transition
        2) Updates the global state to the next valid global state as per
        transition rules.
        Returns an int code indicating whether or not the globals state is
        updated.
        """
        self.__logger.debug(f'current global state before update: '
                            f'{self.current_global_state()}')
        # 1) fetch all components from registry
        all_components = self.find_all()
        components_with_states = self.components_with_state(all_components)

        # get next valid global state
        next_valid_global_state =\
            self.__state_transition_validator.next_valid_global_state(
                all_components,
                components_with_states,
                self.are_all_statuses_up,
                self.are_all_have_same_state)

        # check if globals state is already updated by global health monitor
        if self.current_global_state() == next_valid_global_state:
            self.__logger.debug("global state is already up-to-date")
            return Response.OK

        # 3) otherwise, update the global state
        return self.__update_global_state(next_valid_global_state)

    def update_local_state(self, component, input_command):
        """
        updates the current state of the given component in registry.

        Parameters
        ----------
        component : ServiceComponent
            proxy to service component whose local state is to be updated

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
        next_legal_state = self.__next_valid_local_state(
            current_state, input_command)

        # Case a: transition rule is not valid and local state is ERROR
        if next_legal_state == STATES.ERROR:
            self.__update_local_state(component, next_legal_state)
            # log exception with traceback
            self.__log_exception_with_traceback("invalid transition rule. "
                                                "local state is transited to: "
                                                f"{next_legal_state.name}")
            # return with ERROR to terminate
            return Response.ERROR

        # Case b: transition rule is valid
        # update component's local state to next legal state
        return self.__update_local_state(component, next_legal_state)

    def components_with_status_down(self, all_components):
        """
        Filters the components with status 'DOWN' from the list of given
        components and returns a list of them.

        Parameters
        ----------
        all_components : list
            the list of components to be filtered by status

        Returns
        -------
        components_not_running: list
            list of components which status 'DOWN'
        """
        components_not_running = []
        for component in all_components:
            self.__logger.debug(f'{component.name} status: '
                                f'{component.current_status}.')
            if component.current_status == SERVICE_COMPONENT_STATUS.DOWN:
                components_not_running.append(component)
        return components_not_running

    def update_state_transition_history(self, state_before_transition,
                                        input_command, state_after_transition):
        """
        creates a state transition record and append it to the local state
        transition history.
        """
        # create a state transition record
        self.__local_state_transition_record = self.__create_local_state_transition_record(
            state_before_transition, input_command, state_after_transition)
        self.__local_state_transition_history.append(self.__local_state_transition_record)
