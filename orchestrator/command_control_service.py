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
import zmq

from common.utils import networking_utils
from EBRAINS_RichEndpoint.Application_Companion.common_enums import EVENT, PUBLISHING_TOPIC
from EBRAINS_RichEndpoint.Application_Companion.common_enums import Response
from EBRAINS_RichEndpoint.Application_Companion.common_enums import SERVICE_COMPONENT_CATEGORY
from EBRAINS_RichEndpoint.Application_Companion.common_enums import SERVICE_COMPONENT_STATUS
from EBRAINS_RichEndpoint.Application_Companion.common_enums import SteeringCommands
from EBRAINS_RichEndpoint.orchestrator.communication_endpoint import Endpoint
from EBRAINS_RichEndpoint.orchestrator.communicator_queue import CommunicatorQueue
from EBRAINS_RichEndpoint.orchestrator.proxy_manager_client import ProxyManagerClient
from EBRAINS_RichEndpoint.orchestrator.zmq_sockets import ZMQSockets
from EBRAINS_RichEndpoint.orchestrator.communicator_zmq import CommunicatorZMQ


class CommandControlService(multiprocessing.Process):
    """
    It channels the command and steering between Orchestrator and
    Application Companions.
    """
    def __init__(self, log_settings, configurations_manager,
                 proxy_manager_connection_details,
                 port_range=None
                 ):
        multiprocessing.Process.__init__(self)
        self._log_settings = log_settings
        self._configurations_manager = configurations_manager
        self.__logger = self._configurations_manager.load_log_configurations(
                                        name=__name__,
                                        log_configurations=self._log_settings)
        
        # get client to Proxy Manager Server
        self._proxy_manager_client = ProxyManagerClient(
            self._log_settings,
            self._configurations_manager)

        # Connect with Proxy Manager Server
        # NOTE: it terminates with RuntimeError if connection could ne be made
        # for whatever reasons
        self._proxy_manager_client.connect(
            proxy_manager_connection_details["IP"],
            proxy_manager_connection_details["PORT"],
            proxy_manager_connection_details["KEY"],
        )

        # Now, initialize the proxy to registry manager
        self.__health_registry_manager_proxy =\
            self._proxy_manager_client.get_registry_proxy()
        # initialize the Communicator object for communication via Queues
        self.__communicator = None
        # a list of application companions input queues proxies
        self.__application_companions_in_queues = []
        # a list of application companions output queues proxies
        self.__application_companions_out_queues = []
        self.__communicator = None
        self.__port_range = port_range
        self.__endpoints_address = None
        self.__rep_endpoint_with_orchestrator = None
        self.__publish_endpoint_with_application_companions = None
        self.__pull_endpoint_with_application_companions = None
        self.__topic_publish_steering = PUBLISHING_TOPIC.STEERING.value
        # flag indicating whether the push-pull channel is already established
        # with Application Companions for receiving the responses
        self.__is_pull_connection_with_application_companion_made = False

        self.__logger.debug("C&S service initialized.")

    def __setup_endpoints(self, ports_for_command_control_channel):
        # if the range of ports are not provided then use the shared queues
        # assuming that it is to be deployed on laptop/single node
        if ports_for_command_control_channel is None:
            # proxies to the shared queues
            # for in-coming messages
            self.__queue_in = multiprocessing.Manager().Queue()
            # for out-going messages
            self.__queue_out = multiprocessing.Manager().Queue()
            self.__endpoints_address = (self.__queue_in, self.__queue_out)
            return Response.OK
        else:
            # create ZMQ endpoints
            self.__zmq_sockets = ZMQSockets(self._log_settings, self._configurations_manager)
            # Endpoint with Orchestrator via a REP socket
            self.__rep_endpoint_with_orchestrator = self.__zmq_sockets.create_socket(zmq.REP)
            # Endpoint with Application Companions for sending commands
            # via a PUB socket
            self.__publish_endpoint_with_application_companions = self.__zmq_sockets.create_socket(zmq.PUB)

            self.__my_ip = networking_utils.my_ip()  # get IP address

            # get the port bound to REP socket to communicate with Orchestrator
            port_with_orchestrator =\
                self.__zmq_sockets.bind_to_first_available_port(
                    zmq_socket=self.__rep_endpoint_with_orchestrator,
                    ip=self.__my_ip,
                    min_port=ports_for_command_control_channel['MIN'],
                    max_port=ports_for_command_control_channel['MAX'],
                    max_tries=ports_for_command_control_channel['MAX_TRIES']
                )
            
            # check if port could not be bound
            if port_with_orchestrator is Response.ERROR:
                # NOTE the relevant exception with traceback is already logged
                # by callee function
                self.__logger.error("port with Orchestrator could not be bound")
                # return with error to terminate
                return Response.ERROR

            # get port bound to PUB socket to communicate with Application
            # Companion
            port_with_application_companions =\
                self.__zmq_sockets.bind_to_first_available_port(
                    zmq_socket=self.__publish_endpoint_with_application_companions,
                    ip=self.__my_ip,
                    min_port=ports_for_command_control_channel['MIN'],
                    max_port=ports_for_command_control_channel['MAX'],
                    max_tries=ports_for_command_control_channel['MAX_TRIES']
                )

            # check if port could not be bound
            if port_with_application_companions is Response.ERROR:
                # NOTE the relevant exception with traceback is already logged
                # by callee function
                self.__logger.error("port with Application Companion could "
                                    "not be bound")
                # return with error to terminate
                return Response.ERROR

            # everything went well, now create endpoints address
            endpoint_address_with_orchestrator =\
                Endpoint(self.__my_ip, port_with_orchestrator)

            endpoint_address_with_application_companions =\
                Endpoint(self.__my_ip, port_with_application_companions)
            self.__endpoints_address = {
                SERVICE_COMPONENT_CATEGORY.ORCHESTRATOR: endpoint_address_with_orchestrator,
                SERVICE_COMPONENT_CATEGORY.APPLICATION_COMPANION: endpoint_address_with_application_companions}

            return Response.OK

    def __broadcast_fatal_and_terminate(self,
                                        application_companions_in_queues):
        '''
        broadcasts FATAL to all Application Companions before termination.
        '''
        self.__logger.critical('quitting forcefully!')
        self.__communicator.broadcast_all(
            EVENT.FATAL,
            self.__publish_endpoint_with_application_companions,
            self.__topic_publish_steering)
        # return with error to terminate
        return Response.ERROR

    def __register_with_registry(self):
        """
        helper function for setting up the runtime such as register with
        registry, initialize the Communicator object, etc.
        """
        # register with registry
        if(self.__health_registry_manager_proxy.register(
                    os.getpid(),  # id
                    SERVICE_COMPONENT_CATEGORY.COMMAND_AND_CONTROL,  # name
                    SERVICE_COMPONENT_CATEGORY.COMMAND_AND_CONTROL,  # category
                    self.__endpoints_address,  # endpoint
                    SERVICE_COMPONENT_STATUS.UP,  # current status
                    None) == Response.OK):  # current state
            self.__logger.debug('Command and steering service is registered.')
            return Response.OK
        else:
            # quit with ERROR if not registered
            return Response.ERROR

    def __setup_queue_channeling_to_application_companions(self):
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

    def __setup_channel_receive_response_from_app_companion(self):
        # create a pull socket for receiving responses from Application
        # Companions
        self.__pull_endpoint_with_application_companions =\
            self.__zmq_sockets.create_socket(zmq.PULL)
        self.__application_companions = self.__health_registry_manager_proxy.\
            find_all_by_category(
                            SERVICE_COMPONENT_CATEGORY.APPLICATION_COMPANION)
        self.__logger.debug(f'found application companions: '
                            f'{len( self.__application_companions)}')

        # connect with Application Companions
        for application_companion in self.__application_companions:
            # fetch Application Companion endpoint to connect with
            application_companion_endpoint = application_companion.endpoint[
                    SERVICE_COMPONENT_CATEGORY.COMMAND_AND_CONTROL]
            # format address string
            address = ("tcp://"
                       f"{application_companion_endpoint.IP}:"
                       f"{application_companion_endpoint.port}")
            self.__pull_endpoint_with_application_companions.connect(address)
            self.__logger.info("C&C channel - connected with "
                               "Application Companion at "
                               f"{address} to receive responses")

        return Response.OK

    def __setup_communicator(self):
        """
        helper function to set up a generic communicator to encapsulate the
        underlying tech going to be used for communication.
        """
        if self.__port_range is None:  # communicate via shared queues
            self.__communicator = CommunicatorQueue(
                self._log_settings,
                self._configurations_manager)
        else:  # communicate via 0MQs
            self.__communicator = CommunicatorZMQ(
                self._log_settings,
                self._configurations_manager)

    def __collect_and_forward_responses(self):
        '''
        collects the responses from Application Companions and send them to
        Orchestrator.
        '''
        # setup channel with Application Companions to receive responses if it
        # is not made yet
        if self.__is_pull_connection_with_application_companion_made is False:
            self.__setup_channel_receive_response_from_app_companion()
            # set flag to indicate channel setup with Application Compnions
            self.__is_pull_connection_with_application_companion_made = True
        
        # collect responses
        responses = []  # temporary list for response collection
        # for application_companion_out_queue in \
        #         self.__application_companions_out_queues:
        #     responses.append(self.__communicator.receive(
        #                     application_companion_out_queue))
        for _ in self.__application_companions:
            responses.append(self.__communicator.receive(
                             self.__pull_endpoint_with_application_companions))                          
        # send responses back to orchestrator via zeromq
        self.__logger.debug(f"received responses: {responses}")
        return self.__communicator.send(responses,
                                            self.__rep_endpoint_with_orchestrator)

        # send responses back to orchestrator
        # return self.__communicator.send(
        #                 responses,
        #                 self.__command_and_steering_service_out_queue)

    def __channel_command_and_control(self):
        '''
        Keeps channeling for sending and receiving the steering and control
        commands until successfully terminates or forcefully quits.
        '''
        # The main operational loop for channeling the command and control
        while True:
            # 1. fetch steering command
            current_steering_command = self.__communicator.receive(
                self.__rep_endpoint_with_orchestrator)
            self.__logger.info(f'command received: {current_steering_command.name}')

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
            self.__logger.info(f'Broadcasting {current_steering_command.name}')
            if self.__communicator.broadcast_all(
                    current_steering_command,
                    self.__publish_endpoint_with_application_companions,
                    self.__topic_publish_steering) == Response.ERROR:
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
            self.__logger.debug(f'broadcasted command: {current_steering_command.name}')

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
                self.__logger.info('Concluding Command and Control channelling')
                # exit with OK, after END command execution
                return Response.OK

            # Otherwise continue channelling
            continue

    def run(self):
        # 1. setup communication endpoints with Orchestrator and Application
        # Companions
        if self.__setup_endpoints(self.__port_range) == Response.ERROR:
            try:
                # raise an exception
                raise RuntimeError
            except RuntimeError:
                # log the exception with traceback
                self.__logger.exception('Failed to create endpoints. '
                                        'Quitting!')
            # terminate with ERROR
            return Response.ERROR

        # 2. register with registry
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

        # 3. initialize communicator
        self.__setup_communicator()

        # 4. start channeling command and control
        return self.__channel_command_and_control()
