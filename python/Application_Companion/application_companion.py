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
import multiprocessing
import os
import signal
from signal_manager import SignalManager
from application_manager import ApplicationManager
from common_enums import EVENT
from common_enums import SteeringCommands
from common_enums import Response
from common_enums import SERVICE_COMPONENT_CATEGORY
from common_enums import SERVICE_COMPONENT_STATUS
from python.orchestrator.state_enums import STATES
from python.orchestrator.communicator_queue import CommunicatorQueue


class ApplicationCompanion(multiprocessing.Process):
    """
    It executes the integrated application as a child process
    and controls its execution flow as per the steering commands.
    """

    def __init__(self, log_settings, configurations_manager,
                 actions, component_service_registry_manager):
        multiprocessing.Process.__init__(self)
        self._log_settings = log_settings
        self._configurations_manager = configurations_manager
        self.__logger = self._configurations_manager.load_log_configurations(
                                        name=__name__,
                                        log_configurations=self._log_settings)
        # proxies to point the shared queues
        self.__application_companion_in_queue =\
            multiprocessing.Manager().Queue()
        self.__application_companion_out_queue =\
            multiprocessing.Manager().Queue()
        self.__actions = actions
        self.__application_manager_response_queue = multiprocessing.Queue()
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
        self.__stop_event = self.__signal_manager.shut_down_event
        self.__kill_event = self.__signal_manager.kill_event
        # registry service manager
        self.__component_service_registry_manager =\
            component_service_registry_manager
        self.__is_registered = multiprocessing.Event()
        self.__communicator = None
        self.__ac_registered_component_service = None
        self.__logger.debug("Application Companion is initialized")

    @property
    def is_registered_in_registry(self): return self.__is_registered

    def __set_up(self):
        """
        helper function for setting up.
        """
        # register with registry
        if(self.__component_service_registry_manager.register(
                    os.getpid(),
                    self.__actions['action'],
                    SERVICE_COMPONENT_CATEGORY.APPLICATION_COMPANION,
                    (self.__application_companion_in_queue,
                     self.__application_companion_out_queue),
                    SERVICE_COMPONENT_STATUS.UP,
                    STATES.READY) == Response.OK):
            self.__is_registered.set()
            self.__logger.info('registered with registry.')
        else:
            self.__logger.critical('quitting! could not be registered.')
            return Response.ERROR
        # retreive registered component which is later needed to update states
        self.__ac_registered_component_service = \
            self.__component_service_registry_manager.find_by_id(os.getpid())
        self.__logger.debug(
            f'component service id: '
            f'{self.__ac_registered_component_service.id};'
            f'name: {self.__ac_registered_component_service.name}')
        # initialize the Communicator object for communication via Queues
        self.__communicator = CommunicatorQueue(self._log_settings,
                                                self._configurations_manager)

    def run(self):
        """
        represents the application companion's main activity.
        executes the steering commands and runs the application.
        """
        # setup the necessary settings
        if self.__set_up() is Response.ERROR:
            self.__logger.error('setup failed!.')
            return Response.ERROR
        else:
            while True:
                # fetch the steering command
                current_steering_command = self.__communicator.receive(
                                        self.__application_companion_in_queue)
                self.__logger.debug(f"got the event {current_steering_command}"
                                    " in {self.__event_state_queue}!")
                # got the command to terminate forcefully
                if current_steering_command == EVENT.FATAL:
                    self.__logger.critical('quitting forcefully!')
                    return Response.ERROR
                if(current_steering_command == SteeringCommands.INIT.value):
                    # INIT steering command
                    self.__logger.info('Executing INIT command!')
                    # update local state
                    self.__component_service_registry_manager.update_state(
                                        self.__ac_registered_component_service,
                                        STATES.SYNCHRONIZING)
                    self.__logger.debug(
                     f'current_state: '
                     f'{self.__ac_registered_component_service.current_state}')
                    # initialize Application Manager
                    application_manager = ApplicationManager(
                                    self._log_settings,
                                    self._configurations_manager,
                                    self.__actions,
                                    self.__application_manager_response_queue,
                                    self.__stop_event,
                                    self.__kill_event,
                                    enable_resource_usage_monitoring=True)
                    self.__logger.debug('sending step-size to orchestrator.')
                    # mock-up of sending step-size as response to INIT command
                    self.__communicator.send(
                                    {'pid': os.getpid(), 'min_delay': 0.05},
                                    self.__application_companion_out_queue)
                    self.__logger.info('Executed successfully INIT command!')

                if(current_steering_command == SteeringCommands.START.value):
                    # START steering command
                    self.__logger.info('Executing START command!')
                    # update local state
                    self.__component_service_registry_manager.update_state(
                                        self.__ac_registered_component_service,
                                        STATES.RUNNING)
                    self.__logger.debug(
                     f'current_state: '
                     f'{self.__ac_registered_component_service.current_state}')
                    # start executing the application execution
                    application_manager.start()
                    response = self.__communicator.receive(
                                    self.__application_manager_response_queue)
                    self.__logger.debug(f'Application Manager response: '
                                        f'{response}')
                    self.__logger.debug('sending response to orchestrator.')
                    self.__communicator.send(
                                    response,
                                    self.__application_companion_out_queue)
                    self.__logger.info('Executed successfully START command')

                if(current_steering_command == SteeringCommands.END.value):
                    # END steering command
                    self.__logger.info('Executing END command!')
                    # update local state
                    self.__component_service_registry_manager.update_state(
                                        self.__ac_registered_component_service,
                                        STATES.TERMINATED)
                    self.__logger.debug(
                     f'current_state: '
                     f'{self.__ac_registered_component_service.current_state}')
                    self.__logger.debug('sending response to orchestrator.')
                    self.__communicator.send(
                                    Response.OK,
                                    self.__application_companion_out_queue)
                    self.__logger.info('Executed successfully END command')
                    break
        Response.OK
