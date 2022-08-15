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

from common.utils import proxy_manager_server_utils
from EBRAINS_RichEndpoint.Application_Companion.common_enums import EVENT
from EBRAINS_RichEndpoint.Application_Companion.common_enums import Response
from EBRAINS_RichEndpoint.Application_Companion.common_enums import SERVICE_COMPONENT_CATEGORY
from EBRAINS_RichEndpoint.Application_Companion.common_enums import SERVICE_COMPONENT_STATUS
from EBRAINS_RichEndpoint.Application_Companion.common_enums import SteeringCommands
from EBRAINS_RichEndpoint.orchestrator.communicator_queue import CommunicatorQueue
from EBRAINS_RichEndpoint.orchestrator.proxy_manager_client import ProxyManagerClient


class CommandControlService(multiprocessing.Process):
    """
    It channels the command and steering between Orchestrator and
    Application Companions.
    """
    def __init__(self, log_settings, configurations_manager):
        multiprocessing.Process.__init__(self)
        self._log_settings = log_settings
        self._configurations_manager = configurations_manager
        self.__logger = self._configurations_manager.load_log_configurations(
                                        name=__name__,
                                        log_configurations=self._log_settings)
        # proxies to the shared queues
        self.__command_and_steering_service_in_queue =\
            multiprocessing.Manager().Queue()   # for in-comming messages
        self.__command_and_steering_service_out_queue =\
            multiprocessing.Manager().Queue()   # for out-going messages
        
       # get client to Proxy Manager Server
        self._proxy_manager_client =  ProxyManagerClient(
            self._log_settings,
            self._configurations_manager)

        # Connect with Proxy Manager Server
        # NOTE: it terminates with RuntimeError if connection could ne be made
        # for whatever reasons
        self._proxy_manager_client.connect(
            proxy_manager_server_utils.IP,
            proxy_manager_server_utils.PORT,
            proxy_manager_server_utils.KEY,
        )

        # Now, get the proxy to registry manager
        self.__health_registry_manager_proxy =\
            self._proxy_manager_client.get_registry_proxy()

        # flag to indicate whether C&S service
        # is registered with registry service
        self.__is_registered = multiprocessing.Event()
        # initialize the Communicator object for communication via Queues
        self.__communicator = None
        # create a list of application companions input queues proxies
        self.__application_companions_in_queues = []
        # create a list of application companions output queues proxies
        self.__application_companions_out_queues = []
        self.__logger.debug("C&S service initialized.")

    @property
    def is_registered_in_registry(self): return self.__is_registered

    def __broadcast_fatal_and_terminate(self,
                                        application_companions_in_queues):
        '''
        broadcasts FATAL to all Application Companions before termination.
        '''
        self.__logger.critical('quitting forcefully!')
        self.__communicator.broadcast_all(EVENT.FATAL,
                                          application_companions_in_queues)
        # return with error to terminate
        return Response.ERROR

    def __register_with_registry(self):
        """
        helper function for setting up the runtime such as
        register with registry, initialize the Communicator object, etc.
        """
        # register with registry
        if(self.__health_registry_manager_proxy.register(
                    os.getpid(),  # id
                    SERVICE_COMPONENT_CATEGORY.COMMAND_AND_CONTROL,  # name
                    SERVICE_COMPONENT_CATEGORY.COMMAND_AND_CONTROL,  # category
                    (self.__command_and_steering_service_in_queue,  # endpoint
                     self.__command_and_steering_service_out_queue),
                    SERVICE_COMPONENT_STATUS.UP,  # current status
                    None) == Response.OK):  # current state
            # indicate a successful registration
            self.__is_registered.set()
            self.__logger.debug('Command and steering service is registered.')
            return Response.OK
        else:
            # quit with ERROR if not registered
            return Response.ERROR

    def __setup_channeling_to_application_companions(self):
        '''sets up channeling to application companions for communication.'''
        # fetch proxies for application companions for communication
        application_companions = self.__health_registry_manager_proxy.\
            find_all_by_category(
                            SERVICE_COMPONENT_CATEGORY.APPLICATION_COMPANION)
        self.__logger.debug(f'found application companions: '
                            f'{len(application_companions)}')
        # create a list of application companions input endpoints proxies
        self.__application_companions_in_queues = []
        # create a list of application companions output endpoints proxies
        self.__application_companions_out_queues = []
        # populate the lists
        for application_companion in application_companions:
            in_queue, out_queue = application_companion.endpoint
            self.__application_companions_in_queues.append(in_queue)
            self.__application_companions_out_queues.append(out_queue)

        # Case a, proxies to endpoints are not found
        if not self.__application_companions_in_queues or\
                not self.__application_companions_out_queues:
            # return with error
            return Response.ERROR

        # Case b, proxies to endpoints are found
        self.__logger.debug(f'found application companion endpoints: '
                            f'{len(self.__application_companions_in_queues)}')
        return Response.OK

    def __collect_and_forward_responses(self):
        '''
        collects the responses from Application Companions and send them to
        Orchestrator.
        '''
        responses = []  # temporary list for response collection
        # collect responses
        for application_companion_out_queue in \
                self.__application_companions_out_queues:
            responses.append(self.__communicator.receive(
                            application_companion_out_queue))
        # send responses back to orchestrator
        return self.__communicator.send(
                        responses,
                        self.__command_and_steering_service_out_queue)

    def __channel_command_and_control(self):
        '''
        Keeps channeling for sending and receiving the steering and control
        commands until successfully terminates or forcefully quits.
        '''
        # The main operational loop for channeling the command and control
        while True:
            # 1. fetch steering command
            current_steering_command = self.__communicator.receive(
                            self.__command_and_steering_service_in_queue)
            self.__logger.debug(f'command {current_steering_command}'
                                f' is received.')

            # Case, STATE_UPDATE_FATAL event is received
            if current_steering_command == EVENT.STATE_UPDATE_FATAL:
                # NOTE exception with traceback is logged at source of failure
                self.__logger.critical('quitting forcefully!')
                # terminate with ERROR
                return Response.ERROR

            # Case, FATAL event is received
            if current_steering_command == EVENT.FATAL:
                # NOTE exception with traceback is logged at source of failure
                # broadcast and terminate with error
                return self.__broadcast_fatal_and_terminate(
                    self.__application_companions_in_queues)

            # Case, a steering command is received.
            # 2. broadcast the steering command
            self.__logger.info(f'Broadcasting command: '
                               f'{current_steering_command}')
            if self.__communicator.broadcast_all(
                current_steering_command,
                    self.__application_companions_in_queues) == Response.ERROR:
                try:
                    # broadcast failed, raise an exception
                    raise RuntimeError
                # NOTE relevant exception is already logged by Communicator
                except Exception:
                    # log the exception with traceback
                    self.__logger.exception('could not broadcast.')
                # TODO: handle broadcast failure, maybe retry after a while
                # for now, terminate with error
                self.__logger.critical('Terminating channeling.')
                return Response.ERROR

            # Case, steering command is broadcasted successfully
            self.__logger.debug(f'command: {current_steering_command} is'
                                f' broadcasted.')

            # 3. collect and send responses to Orchestrator
            self.__logger.debug('collecting response.')
            # Case a, response forwarding failed
            if self.__collect_and_forward_responses() == Response.ERROR:
                try:
                    raise RuntimeError
                # NOTE relevant exception is already logged by Communicator
                except Exception:
                    # log the exception with traceback
                    self.__logger.exception('could not send response to '
                                            'Orchestrator.')
                # TODO: handle response forwarding failure, maybe retry
                # after a while
                # for now, terminate with error
                self.__logger.critical('Terminating channelling.')
                return Response.ERROR

            # Case b, responses are forwarded successfully
            self.__logger.debug('forwarded responses to Orchestrator.')

            # 4. Terminate the loop if the broadcasted steering command was
            # END command
            if current_steering_command == SteeringCommands.END.value:
                self.__logger.info('Concluding Command and Control channelling.')
                # exit with OK, after END command execution
                return Response.OK

            # Otherwise continue channelling
            continue

    def run(self):
        # 1. register with registry
        if self.__register_with_registry() == Response.ERROR:
            try:
                # raise an exception
                raise RuntimeError
            except RuntimeError:
                # log the exception with traceback
                self.__logger.exception('Command and Control service could '
                                        'not be registered. Quitting!')
            # terminate with ERROR
            return Response.ERROR

        # 2. setup channels to application companions for communication
        if self.__setup_channeling_to_application_companions() ==\
                Response.ERROR:
            try:
                # raise an exception
                raise RuntimeError
            except RuntimeError:
                # log the exception with traceback
                self.__logger.exception('fails to retrieve proxies to '
                                        'Application Companions endpoints.'
                                        ' Quitting!')
            # terminate with ERROR
            return Response.ERROR

        # 3. initialize communicator
        self.__communicator = CommunicatorQueue(self._log_settings,
                                                self._configurations_manager)
        # 4. start channeling command and control
        return self.__channel_command_and_control()
