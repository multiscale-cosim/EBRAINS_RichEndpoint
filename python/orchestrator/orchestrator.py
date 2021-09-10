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
# ------------------------------------------------------------------------------
import multiprocessing
import os
import signal
from python.Application_Companion.signal_manager import SignalManager
from python.Application_Companion.common_enums import EVENT
from python.Application_Companion.common_enums import SteeringCommands
from python.Application_Companion.common_enums import Response
from python.Application_Companion.common_enums import SERVICE_COMPONENT_CATEGORY
from python.Application_Companion.common_enums import SERVICE_COMPONENT_STATUS
from python.orchestrator.state_enums import STATES
from python.orchestrator.communicator_queue import CommunicatorQueue
from python.orchestrator.health_status_keeper import HealthStatusKeeper
from python.orchestrator.signal_monitor import SignalMonitor


class Orchestrator(multiprocessing.Process):
    def __init__(self, log_settings, configurations_manager,
                 component_service_registry_manager):
        multiprocessing.Process.__init__(self)
        self._log_settings = log_settings
        self._configurations_manager = configurations_manager
        self.__logger = self._configurations_manager.load_log_configurations(
                                        name=__name__,
                                        log_configurations=self._log_settings)
        # settings for singal handling
        self.__signal_manager = SignalManager(
                                  log_settings=log_settings,
                                  configurations_manager=configurations_manager
                                  )
        signal.signal(signal.SIGINT,
                      self.__signal_manager.interrupt_signal_handler
                      )
        signal.signal(signal.SIGTERM,
                      self.__signal_manager.kill_signal_handler
                      )
        signal.signal(signal.SIGALRM,
                      self.__signal_manager.alarm_signal_handler
                      )
        self.__stop_event = self.__signal_manager.shut_down_event
        self.__kill_event = self.__signal_manager.kill_event
        self.__alarm_event = self.__signal_manager.alarm_event
        # proxies to point the shared queues
        self.__orchestrator_in_queue =\
            multiprocessing.Manager().Queue()  # for in-comming messages
        self.__orchestrator_out_queue =\
            multiprocessing.Manager().Queue()  # for out-going messages
        # registry service manager
        self.__component_service_registry_manager =\
            component_service_registry_manager
        # flag to indicate whether Orchestrator is registered with registry
        self.__is_registered = multiprocessing.Event()
        # instantiate global health and status manager object
        self.__health_status_keeper = HealthStatusKeeper(
                                    self._log_settings,
                                    self._configurations_manager,
                                    self.__component_service_registry_manager)
        self.__alarm_signal_monitor = SignalMonitor(
                                        self._log_settings,
                                        self._configurations_manager,
                                        self.__health_status_keeper,
                                        self.__alarm_event)
        self.__step_sizes = None
        self.__response_codes = []
        self.__command_and_steering_service = []
        self.__command_and_steering_service_in_queue = None
        self.__command_and_steering_service_out_queue = None
        self.__orchestrator_registered_component = None
        self.__communicator = None
        self.__logger.debug("initialized.")

    @property
    def is_registered_in_registry(self): return self.__is_registered

    def __get_component_from_registry(self,
                                      target_components_category) -> list:
        """
        helper function for retreiving the proxy of
        registered components by category.

        Parameters
        ----------
        target_components_category : SERVICE_COMPONENT_CATEGORY.Enum
            Category of target service components

        Returns
        ------
        components: list
            list of components which havecategory as target_components_category
        """
        components = self.__component_service_registry_manager.\
            find_all_by_category(target_components_category)
        self.__logger.debug(
            f'found components: {len(components)}')
        return components

    def __update_local_state(self, registered_component, state):
        """
        helper function for updating the local state.

         Parameters
        ----------
        registered_component : ServiceComponent
            component registered in registry.

        state : STATES.Enum
            the new state of the component.

        Returns
        ------
        response code: int
            response code indicating whether or not the state is updated.
        """
        return self.__component_service_registry_manager.update_state(
                                                        registered_component,
                                                        state)

    def __find_minimum_step_size(self, step_sizes_with_pids):
        """
        helper function for finding the minimum step size.

         Parameters
        ----------
        step_sizes_with_pids : list
            list of dictionaries contaning the PIDs and step sizes.

        Returns
        ------
        minimum step size: float
            the minimum step size of the list.
        """
        # extract all step sizes from dictionary
        step_sizes = [sub['min_delay']
                      for sub in step_sizes_with_pids]
        self.__logger.debug(f'step_sizes: {step_sizes}')
        return min(step_sizes)

    def __execute_steering_command(self, steering_command):
        """
        helper function for steering and command executions.
        """
        self.__logger.debug(f'Executing steering command: {steering_command}!')
        # send steering command to C&S service
        self.__communicator.send(steering_command,
                                 self.__command_and_steering_service_in_queue)
        self.__logger.debug('getting the response.')
        if steering_command == SteeringCommands.INIT:
            self.__step_sizes = self.__communicator.receive(
                    self.__command_and_steering_service_out_queue)
            # find the minimum step-size
            self.__logger.debug(f'step_sizes and PIDs: {self.__step_sizes}')
            min_step_size = self.__find_minimum_step_size(self.__step_sizes)
            self.__logger.debug(f'minimum step_size: {min_step_size}')
        else:
            # keep track of responses
            self.__response_codes.append(
                self.__communicator.receive(
                    self.__command_and_steering_service_out_queue))
            self.__logger.debug(f'got the response: {self.__response_codes}')
        self.__logger.debug(f'Successfully executed the command:'
                            f'{steering_command}')

    def __execute_if_validated(self, steering_command, valid_state, new_state):
        if self.__health_status_keeper.current_global_state() ==\
                        valid_state:
            # update local state
            self.__update_local_state(
                self.__orchestrator_registered_component, new_state)
            # send steering command to Application Companions
            self.__execute_steering_command(steering_command)
            # update global state
            self.__health_status_keeper.update_global_state()
            return Response.OK
        else:
            self.__logger.critical(
                f'Global state must be {valid_state} for executing'
                f'the steering command: {steering_command}')
            return Response.ERROR

    def __set_up(self):
        """
        helper function for setting up before orchestration.
        """
        # register with registry
        self.__component_service_registry_manager.register(
                        os.getpid(),  # id
                        SERVICE_COMPONENT_CATEGORY.ORCHESTRATOR,   # category
                        SERVICE_COMPONENT_CATEGORY.ORCHESTRATOR,   # name
                        (self.__orchestrator_in_queue,  # endpoint
                         self.__orchestrator_out_queue),
                        SERVICE_COMPONENT_STATUS.UP,  # current status
                        STATES.READY)  # current state
        # retreive registered component which is later needed to update states
        self.__orchestrator_registered_component =\
            self.__component_service_registry_manager.find_by_id(os.getpid())
        if(self.__orchestrator_registered_component is not None):
            # set the flag to indicate successful registration
            self.__logger.debug(
                f'component service id: '
                f'{self.__orchestrator_registered_component.id}'
                f'; name: {self.__orchestrator_registered_component.name}')
            self.__is_registered.set()
        else:
            # not found in registry
            self.__logger.critical('quitting! not found in registry.')
            signal.raise_signal(signal.SIGTERM)
            return Response.ERROR
        # fetch C&S from regitstry
        self.__command_and_steering_service =\
            self.__get_component_from_registry(
                        SERVICE_COMPONENT_CATEGORY.COMMAND_AND_SERVICE)
        self.__logger.debug(f'command and steering service: '
                            f'{self.__command_and_steering_service[0]}')
        # fetch C&S endpoint (in_queue and out_queue)
        self.__command_and_steering_service_in_queue,\
            self.__command_and_steering_service_out_queue =\
            self.__command_and_steering_service[0].endpoint
        # initialize the Communicator object for communication via Queues
        self.__communicator = CommunicatorQueue(self._log_settings,
                                                self._configurations_manager)
        # update global state to READY assuming all components are already
        # launched by the launcher successfully. The local states are anyway
        # validated during the update process.
        self.__health_status_keeper.update_global_state()
        # start monitoring threads
        self.__health_status_keeper.start_monitoring()
        self.__alarm_signal_monitor.start_monitoring()
        return Response.OK

    def run(self):
        """
        executes the steering and commands, and orchestrates the workflow.
        """
        # setup for orchestration
        if self.__set_up() is Response.ERROR:
            self.__logger.error('setup failed!.')
            return Response.ERROR
        else:
            # orchestrate until successfully terminates or forcefully quit
            while True:
                # terminate if CTRL+C is pressed
                if self.__stop_event.is_set() or self.__kill_event.is_set():
                    self.__logger.critical('quitting forcefully!')
                    # self.__execute_steering_command(SteeringCommands.END)
                    return Response.ERROR
                # terminiate if alarm signal is raised
                # i.e. due to global state ERROR
                if self.__alarm_event.is_set():
                    # confirm one more time to rule out network delay
                    self.__logger.critical('re-checking the global state.')
                    # if it is still ERROR state
                    if self.__health_status_keeper.update_global_state() ==\
                            Response.ERROR:
                        self.__logger.critical('Quitting! ERROR global state.')
                        # send terminate command to Application Companions
                        self.__execute_steering_command(EVENT.FATAL)
                        # stop monitoring
                        self.__health_status_keeper.finalize_monitoring()
                        return Response.ERROR
                    else:
                        # globals state is updated and valid now
                        self.__logger.critical('global state is valid.')
                        # reset the alarm
                        self.__signal_manager.reset_alarm()
                        # restart monitoring
                        self.__health_status_keeper.start_monitoring()
                else:
                    self.__logger.debug(
                        f'current global state: '
                        f'{self.__health_status_keeper.current_global_state()}')
                    # fetch the steering command
                    current_steering_command = self.__communicator.receive(
                                                self.__orchestrator_in_queue)
                    self.__logger.debug(
                                f'got the command {current_steering_command}'
                                f' in {self.__orchestrator_in_queue}!')
                    if(current_steering_command == SteeringCommands.INIT):
                        # validate the local and global states and execute
                        self.__execute_if_validated(current_steering_command,
                                                    STATES.READY,
                                                    STATES.SYNCHRONIZING)
                    if(current_steering_command == SteeringCommands.START):
                        # validate the local and global states and execute
                        self.__execute_if_validated(current_steering_command,
                                                    STATES.SYNCHRONIZING,
                                                    STATES.RUNNING)
                    if(current_steering_command == SteeringCommands.END):
                        # validate the local and global states and execute
                        self.__execute_if_validated(current_steering_command,
                                                    STATES.RUNNING,
                                                    STATES.TERMINATED)
                        # stop monitoring
                        self.__health_status_keeper.finalize_monitoring()
                        break
            return Response.OK
