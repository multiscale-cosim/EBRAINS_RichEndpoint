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
import signal
import os
from python.orchestrator.steering_menu_handler import SteeringMenuCLIHandler
from python.Application_Companion.common_enums import EVENT
from python.Application_Companion.common_enums import Response
from python.Application_Companion.common_enums import SERVICE_COMPONENT_CATEGORY
from python.Application_Companion.common_enums import SERVICE_COMPONENT_STATUS
from python.Application_Companion.common_enums import SteeringCommands
from python.Application_Companion.signal_manager import SignalManager
from python.orchestrator.communicator_queue import CommunicatorQueue


class CommandSteeringService(multiprocessing.Process):
    """
    It channels the command and steering between Orchestrator and
    Application Companions.
    """
    def __init__(self, log_settings, configurations_manager,
                 component_service_registry_manager) -> None:
        multiprocessing.Process.__init__(self)
        self._log_settings = log_settings
        self._configurations_manager = configurations_manager
        self.__logger = self._configurations_manager.load_log_configurations(
                                        name=__name__,
                                        log_configurations=self._log_settings)
        # proxies to point the shared queues
        self.__command_and_steering_service_in_queue =\
            multiprocessing.Manager().Queue()   # for in-comming messages
        self.__command_and_steering_service_out_queue =\
            multiprocessing.Manager().Queue()   # for out-going messages
        # proxy to registry service
        self.__component_service_registry_manager =\
            component_service_registry_manager
        # flag to indicate whether C&S service
        # is registered with registry service
        self.__is_registered = multiprocessing.Event()
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
        # initialize the Communicator object for communication via Queues
        self.__communicator = None
        self.__logger.debug("C&S service initialized.")

    @property
    def is_registered_in_registry(self): return self.__is_registered

    # def __get_steering_menu_item(self, command):
    #     """
    #     maps the steering command with menu item.

    #     Parameters
    #     ----------
    #     command : SteeringCommands.Enum
    #         steering command enum

    #     Returns
    #     ------
    #     menu item: returns the corresponding menu item, if found.
    #     None: if not found.
    #     """
    #     menu_item = self.__steering_menu_handler.get_menu_item(command)
    #     self.__logger.debug(f'selected menu item: {menu_item}')
    #     if menu_item != Response.ERROR:
    #         return menu_item
    #     else:
    #         return None

    def run(self):
        # register with registry
        if(self.__component_service_registry_manager.register(
                    os.getpid(),  # id
                    SERVICE_COMPONENT_CATEGORY.COMMAND_AND_SERVICE,  # category
                    SERVICE_COMPONENT_CATEGORY.COMMAND_AND_SERVICE,  # name
                    (self.__command_and_steering_service_in_queue,  # endpoint
                     self.__command_and_steering_service_out_queue),
                    SERVICE_COMPONENT_STATUS.UP,  # current status
                    None) == Response.OK):  # current state
            # indicate a successful registration
            self.__is_registered.set()
            self.__logger.debug('Command and steering service is registered.')
        else:
            # quit if not registered
            self.__logger.critical('quitting! \
                            command and steering service is not registered.')
            signal.raise_signal(signal.SIGTERM)
            return Response.ERROR
        # fetch proxies for application companions for communication
        application_companions = self.__component_service_registry_manager.\
            find_all_by_category(
                            SERVICE_COMPONENT_CATEGORY.APPLICATION_COMPANION)
        self.__logger.debug(f'found application companions: '
                            f'{len(application_companions)}')
        # careate a list of application companions input queues proxies
        application_companions_in_queues = []
        # careate a list of application companions output queues proxies
        application_companions_out_queues = []
        # populate the lists
        for application_companion in application_companions:
            in_queue, out_queue = application_companion.endpoint
            application_companions_in_queues.append(in_queue)
            application_companions_out_queues.append(out_queue)
        self.__logger.debug(f'found application companion queues: '
                            f'{len(application_companions_in_queues)}')
        self.__communicator = CommunicatorQueue(self._log_settings,
                                                self._configurations_manager)
        while True:
            # the main operational loop for sending and receiving
            # the steering and commands. It runs until successfully
            # terminates or forcefully quits.
            if self.__stop_event.is_set() or self.__kill_event.is_set():
                self.__logger.critical('quitting forcefully!')
                return Response.ERROR
            else:
                # fetch the steering command
                current_steering_command = self.__communicator.receive(
                                self.__command_and_steering_service_in_queue)
                self.__logger.debug(f'command {current_steering_command}'
                                    f'is received.')
                if current_steering_command == EVENT.FATAL:
                    self.__logger.critical('quitting forcefully!')
                    self.__communicator.broadcast_all(
                        current_steering_command,
                        application_companions_in_queues)
                    break
                    # return Response.ERROR
                # broadcast the command
                self.__communicator.broadcast_all(
                    current_steering_command, application_companions_in_queues)
                # # keep track of sent steering commands
                # self.__steering_commands_history.append(
                #     self.__get_steering_menu_item(current_steering_command))
                # self.__logger.debug(f'running command history:'
                #                    f'{self.__steering_commands_history}')
                responses = []  # temporary list for response collection

                # collect responses
                for application_companion_out_queue in application_companions_out_queues:
                    responses.append(
                                self.__communicator.receive(
                                    application_companion_out_queue))
                # send responses back to orchestrator
                self.__communicator.send(
                                responses,
                                self.__command_and_steering_service_out_queue)
                if(current_steering_command != SteeringCommands.END.value):
                    # continue channeling
                    continue
                else:  # exit
                    self.__logger.info('Executing END command!')
                    break
        return Response.OK
