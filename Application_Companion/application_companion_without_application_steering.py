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
from EBRAINS_RichEndpoint.Application_Companion.application_manager import ApplicationManager
from EBRAINS_RichEndpoint.Application_Companion.common_enums import EVENT
from EBRAINS_RichEndpoint.Application_Companion.common_enums import SteeringCommands
from EBRAINS_RichEndpoint.Application_Companion.common_enums import Response
from EBRAINS_RichEndpoint.Application_Companion.common_enums import SERVICE_COMPONENT_CATEGORY
from EBRAINS_RichEndpoint.Application_Companion.common_enums import SERVICE_COMPONENT_STATUS
from EBRAINS_RichEndpoint.registry_state_machine.state_enums import STATES
from EBRAINS_RichEndpoint.orchestrator.communicator_queue import CommunicatorQueue
from EBRAINS_RichEndpoint.Application_Companion.affinity_manager import AffinityManager


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
        # proxies to the shared queues
        self.__application_companion_in_queue =\
            multiprocessing.Manager().Queue()  # for in-comming messages
        self.__application_companion_out_queue =\
            multiprocessing.Manager().Queue()  # for out-going messages
        # for receiving the responses from Application Manager
        self.__application_manager_response_queue =\
            multiprocessing.Manager().Queue()
        # actions (applications) to be launched
        self.__actions = actions
        # registry service manager
        self.__component_service_registry_manager =\
            component_service_registry_manager
        self.__is_registered = multiprocessing.Event()
        # initialize AffinityManager for handling affinity settings
        self.__affinity_manager = AffinityManager(
            self._log_settings, self._configurations_manager)
        # restrict Application Companion to a single core (i.e core 1) only
        # so not to interrupt the execution of the main application
        self.__bind_to_cpu = [0]  # TODO: configure it from configurations file
        self.__communicator = None
        self.__ac_registered_component_service = None
        self.__application_manager = None
        self.__logger.debug("Application Companion is initialized")

    @property
    def is_registered_in_registry(self): return self.__is_registered

    def __set_up_runtime(self):
        """
        helper function for setting up the runtime such as
        register with registry, initialize the Communicator object, etc.
        """
        # 1.  set affinity
        if self.__affinity_manager.set_affinity(
                os.getpid(), self.__bind_to_cpu) == Response.ERROR:
            # Case, affinity is not set
            try:
                # raise run time error exception
                raise RuntimeError
            except RuntimeError:
                # log the exception with traceback
                self.__logger.exception('Affinity could not be set.')

        # 2. register with registry
        if self.__component_service_registry_manager.register(
                os.getpid(),  # id
                self.__actions['action'],  # name
                SERVICE_COMPONENT_CATEGORY.APPLICATION_COMPANION,  # category
                (self.__application_companion_in_queue,  # endpoint
                 self.__application_companion_out_queue),
                SERVICE_COMPONENT_STATUS.UP,  # current status
                # current state
                STATES.READY) == Response.ERROR:
            # Case, registration fails
            try:
                # raise run time error exception
                raise RuntimeError
            except RuntimeError:
                # log the exception with traceback
                self.__logger.exception('Could not be registered. Quitting!')
                # terminate with error
            return Response.ERROR

        # 3. indicate a successful registration
        self.__is_registered.set()
        self.__logger.info('registered with registry.')

        # 4. get proxy to update the states later in registry
        self.__ac_registered_component_service = \
            self.__component_service_registry_manager.find_by_id(os.getpid())
        self.__logger.debug(
                        f'component service id: '
                        f'{self.__ac_registered_component_service.id};'
                        f'name: {self.__ac_registered_component_service.name}')

        # 5. initialize the Communicator object for communication via Queues
        self.__communicator = CommunicatorQueue(self._log_settings,
                                                self._configurations_manager)
        return Response.OK

    def __respond_with_state_update_error(self):
        '''
        i)  informs Orchestrator about local state update failure.
        ii) logs the exception with traceback and terminates loudly with error.
        '''
        # log the exception with traceback details
        try:
            # raise runtime exception
            raise RuntimeError
        except RuntimeError:
            # log the exception with traceback details
            self.__logger.exception('Could not update state. Quiting!')
        # inform Orchestrator about state update failure
        self.__communicator.send(EVENT.STATE_UPDATE_FATAL,
                                 self.__application_companion_out_queue)
        # terminate with error
        return Response.ERROR

    def __update_local_state(self, new_state):
        '''
        updates the local state.

        Parameters
        ----------

        new_state : STATE.enum
            new state value to be updated with.

        Returns
        ------
            return code as int
        '''
        self.__logger.debug(
                f'current state before update: '
                f'{self.__ac_registered_component_service.current_state}')
        rc = self.__component_service_registry_manager.update_state(
                            self.__ac_registered_component_service,
                            new_state)
        self.__logger.debug(
                f'current state after update: '
                f'{self.__ac_registered_component_service.current_state}')
        return rc

    def __send_response_to_orchestrator(self, response):
        '''
        sends response to Orchestrator as a result of Steering Command
        execution.

        Parameters
        ----------

        response : ...
            response to be sent to Orchestrator

        Returns
        ------
            return code as int
        '''
        self.__logger.debug(f'sending {response} to orchestrator.')
        return self.__communicator.send(response,
                                        self.__application_companion_out_queue)

    def __execute_init_command(self):
        '''helper function to execute INIT steering command'''
        self.__logger.info('Executing INIT command!')
        # i. update local state
        if self.__update_local_state(STATES.SYNCHRONIZING) == Response.ERROR:
            # terminate loudly as state could not be updated
            # exception is already logged with traceback
            return self.__respond_with_state_update_error()
        # ii. initialize Application Manager
        self.__application_manager = ApplicationManager(
                        # parameters for setting up the uniform log settings
                        self._log_settings,  # log settings
                        self._configurations_manager,  # Configurations Manager
                        # actions (applications) to be launched
                        self.__actions,
                        # proxy to shared queue to receive responses from
                        # Application Manager
                        self.__application_manager_response_queue,
                        # flag to enable/disable resource usage monitoring
                        # TODO set monitoring enable/disable settings from XML
                        enable_resource_usage_monitoring=True)
        # iii. mock-up of sending the local minimum step-size in response
        self.__logger.debug('sending step-size to orchestrator.')
        self.__send_response_to_orchestrator(
                                    {'pid': os.getpid(),  # PID
                                     'min_delay': 0.05})  # local step size
        self.__logger.info('Executed successfully INIT command!')
        return Response.OK

    def __execute_start_command(self):
        '''helper function to execute START steering command'''
        self.__logger.info('Executing START command!')
        # i. update local state
        if self.__update_local_state(STATES.RUNNING) == Response.ERROR:
            # terminate loudly as state could not be updated
            # exception is already logged with traceback
            return self.__respond_with_state_update_error()

        # ii. start the application execution
        self.__logger.debug('starting executing the application.')
        self.__application_manager.start()
        # iii. receive response from Application Manager
        response = self.__communicator.receive(
                        self.__application_manager_response_queue)
        self.__logger.debug(f'Application Manager response: {response}')
        # iv. send response to orchestrator
        self.__send_response_to_orchestrator(response)
        self.__logger.info('Executed successfully START command')
        return Response.OK

    def __execute_end_command(self):
        '''helper function to execute END steering command'''
        self.__logger.info('Executing END command!')
        # # i. update local state
        if self.__update_local_state(STATES.TERMINATED) == Response.ERROR:
            # terminate loudly as state could not be updated
            # exception is already logged with traceback
            return self.__respond_with_state_update_error()

        # ii. send response to orchestrator
        self.__communicator.send(Response.OK,
                                 self.__application_companion_out_queue)
        self.__logger.info('Executed successfully END command')
        return Response.OK

    def __terminate_with_error(self):
        '''helper function to terminate the execution with error.'''
        self.__logger.critical('rasing signal to terminate with error.')
        # raise signal
        signal.raise_signal(signal.SIGTERM)
        # terminate with error
        return Response.ERROR

    def __handle_fatal_event(self):
        self.__logger.critical('quitting forcefully!')
        # return with ERROR to indicate preemptory exit
        # NOTE an exception is logged with traceback by calling function
        # when return with ERROR
        return Response.ERROR

    def __fetch_and_execute_steering_commands(self):
        '''
        Main loop to fetch and execute the steering commands.
        The loop terminates either by normally or forcefully i.e.:
        i)  Normally: receveing the steering command END, or by
        ii) Forcefully: receiving the FATAL command from Orchestrator.
        '''
        # create a dictionary of choices for the steering commands and
        # their corresponding executions
        command_execution_choices = {
                        SteeringCommands.INIT: self.__execute_init_command,
                        SteeringCommands.START: self.__execute_start_command,
                        SteeringCommands.END: self.__execute_end_command,
                        EVENT.FATAL: self.__handle_fatal_event}

        # loop for executing and fetching the steering commands
        while True:
            # 1. fetch the Steering Command
            current_steering_command = self.__communicator.receive(
                                self.__application_companion_in_queue)
            self.__logger.debug(f'got the command {current_steering_command}')
            # 2. execute the current steering command
            if command_execution_choices[current_steering_command]() ==\
                    Response.ERROR:
                # something went wrong, terminate loudly with error
                try:
                    # raise runtime exception
                    raise RuntimeError
                except RuntimeError:
                    # log the exception with traceback details
                    self.__logger.exception(f'Error executing command: '
                                            f'{current_steering_command}. '
                                            f'Quiting!')
                finally:
                    return self.__terminate_with_error()

            # 3 (a). If END coomand is executed, finish execution as normal
            if current_steering_command == SteeringCommands.END:
                self.__logger.info('Concluding execution.')
                # finish execution as normal
                return Response.OK

            # 3 (b). Otherwise, keep fetching/executing the steering commands
            continue

    def run(self):
        """
        Represents the main activities of the Application Companion
        i.  sets up the runtime settings.
        ii. executes the application and manages the flow
        as per steering commands.
        """
        # i. setup the necessary settings for runtime such as
        # to register with registry, etc.
        if self.__set_up_runtime() is Response.ERROR:
            self.__logger.error('setup failed!.')
            return Response.ERROR

        # ii. loop for fetching and executing the steering commands
        return self.__fetch_and_execute_steering_commands()
