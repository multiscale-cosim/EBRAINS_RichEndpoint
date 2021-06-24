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
import queue
import signal
from signal_manager import SignalManager
from application_manager import ApplicationManager
from common_enums import SteeringCommands, Response


class ApplicationCompanion(multiprocessing.Process):
    """
    It executes the integrated application as a child process
    and duly executes the steering commands.
    """

    def __init__(self, log_settings, configurations_manager,
                 actions, command_steering_queue, ac_response_queue):
        multiprocessing.Process.__init__(self)
        self._log_settings = log_settings
        self._configurations_manager = configurations_manager
        self.__logger = self._configurations_manager.load_log_configurations(
                                        name=__name__,
                                        log_configurations=self._log_settings,
                                        directory='logs',
                                        directory_path='AC results')
        self.__logger.debug("logger is configured.")
        self.__command_and_steering_queue = command_steering_queue
        self.__application_companion_response_queue = ac_response_queue
        self.__actions = actions
        self.__application_manager_response_queue = multiprocessing.Queue()
        # self._ac_pipe, self._am_pipe = multiprocessing.Pipe()
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
        self.__logger.debug("Application Companion is initialized")

    def __get_event(self, event_queue):
        """
        Retrieve the event from the queue.

        Parameters
        ----------
        event_queue : queue
            queue to retrieve the event from

        Returns
        ------
        event from the queue
        """
        while True:
            # wait until an event is retrieved
            # or the process is forcefully quit
            if self.__stop_event.is_set() or self.__kill_event.is_set():
                self.__logger.critical('quitting forcefully!')
                self.__application_companion_response_queue.put(
                                           Response.ERROR)
                break
            else:
                try:
                    # TODO: Configure the timeout value from XML files
                    current_event = event_queue.get(timeout=10)
                except queue.Empty:
                    self.__logger.info(f'waiting for the event in {queue}!')
                    continue
                else:
                    return current_event

    def run(self):
        """
        represents the application companion's main activity.
        executes the steering commands and runs the application.
        """
        while True:
            # run until successfully terminates or forcefully quit
            if self.__stop_event.is_set() or self.__kill_event.is_set():
                self.__logger.critical('quitting forcefully!')
                self.__application_companion_response_queue.put(
                                           Response.ERROR)
                break
            else:
                # fetch the next steering command
                current_steering_command = self.__get_event(
                                            self.__command_and_steering_queue)
                self.__logger.debug(f"got the event {current_steering_command}"
                                    " in {self.__event_state_queue}!")
                if(current_steering_command == SteeringCommands.INIT.value):
                    # INIT steering command
                    self.__logger.info('Executing INIT command!')
                    application_manager = ApplicationManager(
                                    self._log_settings,
                                    self._configurations_manager,
                                    self.__actions,
                                    self.__application_manager_response_queue,
                                    self.__stop_event,
                                    self.__kill_event,
                                    enable_resource_usage_monitoring=True)
                    self.__logger.debug('putting response back')
                    # mock-up of sending min delay as response to INIT command
                    self.__application_companion_response_queue.put(
                                    {'pid': os.getpid(), 'min_delay': 0.05})
                    # indicate the enqueued task (command) is
                    # successfully executed
                    self.__command_and_steering_queue.task_done()
                    self.__logger.info('Executed successfully INIT command!')

                if(current_steering_command == SteeringCommands.START.value):
                    # START steering command
                    self.__logger.info('Executing START command!')
                    application_manager.start()
                    response = self.__get_event(
                                    self.__application_manager_response_queue)
                    self.__logger.debug(f'got the response {response} in\
                            {self.__application_manager_response_queue}!')
                    self.__logger.debug('putting response back')
                    self.__application_companion_response_queue.put(
                                                            Response.OK)
                    # indicate the enqueued task (command)
                    # is successfully executed
                    self.__command_and_steering_queue.task_done()
                    self.__logger.info('Executed successfully START command')

                if(current_steering_command == SteeringCommands.END.value):
                    # START steering command
                    self.__logger.info('Executing END command!')
                    self.__application_companion_response_queue.put(
                        Response.OK)
                    # indicate the enqueued task (command)
                    # is successfully executed
                    self.__command_and_steering_queue.task_done()
                    self.__logger.info('Executed successfully END command')
                    break
