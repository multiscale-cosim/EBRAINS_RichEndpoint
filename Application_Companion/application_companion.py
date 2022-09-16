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
import pickle
import time

from common.utils import networking_utils
from EBRAINS_RichEndpoint.Application_Companion.application_manager import ApplicationManager
from EBRAINS_RichEndpoint.Application_Companion.common_enums import EVENT, PUBLISHING_TOPIC
from EBRAINS_RichEndpoint.Application_Companion.common_enums import SteeringCommands
from EBRAINS_RichEndpoint.Application_Companion.common_enums import Response
from EBRAINS_RichEndpoint.Application_Companion.common_enums import SERVICE_COMPONENT_CATEGORY
from EBRAINS_RichEndpoint.Application_Companion.common_enums import SERVICE_COMPONENT_STATUS
from EBRAINS_RichEndpoint.Application_Companion.affinity_manager import AffinityManager
from EBRAINS_RichEndpoint.registry_state_machine.state_enums import STATES
from EBRAINS_RichEndpoint.orchestrator.communicator_queue import CommunicatorQueue
from EBRAINS_RichEndpoint.orchestrator.communicator_zmq import CommunicatorZMQ
from EBRAINS_RichEndpoint.orchestrator.zmq_sockets import ZMQSockets
from EBRAINS_RichEndpoint.orchestrator.communication_endpoint import Endpoint
from EBRAINS_RichEndpoint.orchestrator.proxy_manager_client import ProxyManagerClient


class ApplicationCompanion(multiprocessing.Process):
    """
    It executes the integrated application as a child process
    and controls its execution flow as per the steering commands.
    """

    def __init__(self, log_settings, configurations_manager, actions,
                 proxy_manager_connection_details,
                 port_range=None):
        multiprocessing.Process.__init__(self)
        self._log_settings = log_settings
        self._configurations_manager = configurations_manager
        self.__logger = self._configurations_manager.load_log_configurations(
            name=__name__, log_configurations=self._log_settings
        )
        # # proxies to the shared queues
        # self.__application_companion_in_queue = (
        #     multiprocessing.Manager().Queue()
        # )  # for in-comming messages
        # self.__application_companion_out_queue = (
        #     multiprocessing.Manager().Queue()
        # )  # for out-going messages
        # for sending the commands to Application Manager
        self.__application_manager_in_queue = multiprocessing.Manager().Queue()
        # for receiving the responses from Application Manager
        self.__application_manager_out_queue = multiprocessing.Manager().Queue()
        # actions (applications) to be launched
        self.__actions = actions

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

        # Now, get the proxy to registry manager
        self.__health_registry_manager_proxy =\
            self._proxy_manager_client.get_registry_proxy()
            
        self.__is_registered = multiprocessing.Event()
        # initialize AffinityManager for handling affinity settings
        self.__affinity_manager = AffinityManager(
            self._log_settings, self._configurations_manager
        )
        self.__port_range = port_range
        # restrict Application Companion to a single core (i.e core 1) only
        # so not to interrupt the execution of the main application
        self.__bind_to_cpu = [0]  # TODO: configure it from configurations file
        self.__communicator = None
        self.__endpoints_address = None
        self.__ac_registered_component_service = None
        self.__application_manager = None
        self.__logger.debug("Application Companion is initialized")

    @property
    def is_registered_in_registry(self):
        return self.__is_registered

    def __get_component_from_registry(self, target_components_category) -> list:
        """
        helper function for retrieving the proxy of registered components by
        category.

        Parameters
        ----------
        target_components_category : SERVICE_COMPONENT_CATEGORY.Enum
            Category of target service components

        Returns
        ------
        components: list
            list of components matches category with parameter
            target_components_category
        """
        components = self.__health_registry_manager_proxy.\
            find_all_by_category(target_components_category)
        self.__logger.debug(
            f'found components: {len(components)}')
        return components

    def __get_command_control_endpoint(self):
        # fetch C&C from registry
        self.__command_and_control_service =\
            self.__get_component_from_registry(
                        SERVICE_COMPONENT_CATEGORY.COMMAND_AND_CONTROL)
        self.__logger.debug(f'command and steering service: '
                            f'{self.__command_and_control_service[0]}')
        # fetch C&C endpoint <ip:port>
        return self.__command_and_control_service[0].endpoint[
            SERVICE_COMPONENT_CATEGORY.APPLICATION_COMPANION]

    def __setup_command_control_channel(self, ports_for_command_control_channel):
        """creates communication endpoints"""
        # if the range of ports are not provided then use the shared queues
        # assuming that it is to be deployed on laptop/single node
        if ports_for_command_control_channel is None:
            # proxies to the shared queues
            # for in-coming messages
            self.__queue_in = (
                multiprocessing.Manager().Queue()
            )
            # for out-going messages
            self.__queue_out = (
                multiprocessing.Manager().Queue()
            )
            self.__endpoints_address = (self.__queue_in, self.__queue_out)
            return Response.OK
        else:
            # create ZMQ endpoints
            zmq_sockets = ZMQSockets(self._log_settings, self._configurations_manager)
            # Endpoint with C&C service for receiving commands via a SUB socket
            self.__subscription_endpoint_with_command_control = zmq_sockets.sub_socket()
            # connect with endpoint (PUB socket) of C&C service
            # fetch C&C endpoint <ip:port>
            command_and_steering_service_endpoint = self.__get_command_control_endpoint()
            # subscribe to topic
            zmq_sockets.subscribe_to_topic(
                self.__subscription_endpoint_with_command_control,
                PUBLISHING_TOPIC.STEERING.value)
            # connect to receive broadcast
            self.__subscription_endpoint_with_command_control.connect(
                "tcp://"  # protocol
                f"{command_and_steering_service_endpoint.IP}:"  # ip
                f"{command_and_steering_service_endpoint.port}"  # port
                )
            self.__logger.info("C&C channel - connected with C&C at "
                               f"{command_and_steering_service_endpoint.IP}:"
                               f"{command_and_steering_service_endpoint.port} "
                               "to receive (broadcast) commands")

            # Endpoint with C&C for sending responses via a PUSH socket
            self.__push_endpoint_with_command_control = zmq_sockets.push_socket()
            self.__my_ip = networking_utils.my_ip()  # get IP address
            # bind PUSH socket to first available port in range for sending the
            # response, and get port bound to PUSH socket to communicate with
            # C&C service
            port_to_push_to_command_control =\
                zmq_sockets.bind_to_first_available_port(
                    zmq_socket=self.__push_endpoint_with_command_control,
                    ip=self.__my_ip,
                    min_port=ports_for_command_control_channel['MIN'],
                    max_port=ports_for_command_control_channel['MAX'],
                    max_tries=ports_for_command_control_channel['MAX_TRIES']
                )

            # check if port could not be bound
            if port_to_push_to_command_control is Response.ERROR:
                # NOTE the relevant exception with traceback is already logged
                # by callee function
                self.__logger.error("port with Command&Control could not be "
                                    "bound")
                # return with error to terminate
                return Response.ERROR

            # everything went well, now create endpoints address
            push_endpoint_with_command_control =\
                Endpoint(self.__my_ip, port_to_push_to_command_control)

            self.__endpoints_address = {
                SERVICE_COMPONENT_CATEGORY.COMMAND_AND_CONTROL:
                    push_endpoint_with_command_control}

            return Response.OK

    def __set_up_runtime(self):
        """
        helper function for setting up the runtime such as
        register with registry, initialize the Communicator object, etc.
        """
        # 1.  set affinity
        if (self.__affinity_manager.set_affinity(
                os.getpid(), self.__bind_to_cpu) == Response.ERROR):
            # Case, affinity is not set
            try:
                # raise run time error exception
                raise RuntimeError
            except RuntimeError:
                # log the exception with traceback
                self.__logger.exception("Affinity could not be set.")
        
        # 1. setup communication endpoints with Orchestrator and Application
        # Companions
        if self.__setup_command_control_channel(self.__port_range) == Response.ERROR:
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
        if (
            self.__health_registry_manager_proxy.register(
                os.getpid(),  # id
                self.__actions["action"],  # name
                SERVICE_COMPONENT_CATEGORY.APPLICATION_COMPANION,  # category
                self.__endpoints_address,  # endpoint
                SERVICE_COMPONENT_STATUS.UP,  # current status
                # current state
                STATES.READY
            ) == Response.ERROR
        ):
            # Case, registration fails
            try:
                # raise run time error exception
                raise RuntimeError
            except RuntimeError:
                # log the exception with traceback
                self.__logger.exception("Could not be registered. Quitting!")
                # terminate with error
            return Response.ERROR

        # 3. indicate a successful registration
        self.__is_registered.set()
        self.__logger.info("registered with registry.")

        # 4. get proxy to update the states later in registry
        self.__ac_registered_component_service = (
            self.__health_registry_manager_proxy.find_by_id(os.getpid())
        )
        self.__logger.debug(
            f"component service id: "
            f"{self.__ac_registered_component_service.id};"
            f"name: {self.__ac_registered_component_service.name}"
        )

        # 5. initialize the Communicator object for communication via Queues
        self.__communicator = CommunicatorQueue(
            self._log_settings, self._configurations_manager
        )

        # 5. initialize the Communicator object for communication via ZMQ
        self.__communicator_zmq = CommunicatorZMQ(
            self._log_settings, self._configurations_manager
        )
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

    def __respond_with_state_update_error(self):
        """
        i)  informs Orchestrator about local state update failure.
        ii) logs the exception with traceback and terminates loudly with error.
        """
        # log the exception with traceback details
        try:
            # raise runtime exception
            raise RuntimeError
        except RuntimeError:
            # log the exception with traceback details
            self.__logger.exception("Could not update state. Quitting!")
        # inform Orchestrator about state update failure
        # self.__send_response_to_orchestrator(
        #     EVENT.STATE_UPDATE_FATAL,
        #     self.__application_companion_out_queue)
        self.__send_response_to_orchestrator(EVENT.STATE_UPDATE_FATAL)
        # terminate with error
        return Response.ERROR

    def __update_local_state(self, input_command):
        """
        updates the local state.

        Parameters
        ----------

        input_command: SteeringCommands.Enum
            the command to transit from the current state to next legal state

        Returns
        ------
            return code as int
        """
        return self.__health_registry_manager_proxy.update_local_state(
            self.__ac_registered_component_service, input_command
        )

    def __send_response_to_orchestrator(self, response):
        """
        sends response to Orchestrator as a result of Steering Command
        execution.

        Parameters
        ----------

        response : ...
            response to be sent to Orchestrator

        Returns
        ------
            return code as int
        """
        self.__logger.debug(f"sending {response} to orchestrator.")
        # return self.__communicator.send(
        #     response, self.__application_companion_out_queue
        # )
        return self.__communicator_zmq.send(response, self.__push_endpoint_with_command_control)

    def __receive_response_from_application_manager(self):
        response = self.__communicator.receive(
                        self.__application_manager_out_queue)
        self.__logger.debug(f"response from Application Manager {response}")
        return response

    def __command_execution_response(self, response, steering_command):
        '''
        helper function to check the response received from Application Manager
        as a response. It could be OK, ERROR or a PID and the local minimum
        stepsize of the simulator.

        Parameters
        ----------

        response : ...
            response received from the Application Manager.
        '''
        # Check response received from Application Manager
        if response == Response.ERROR:
            # Case a. something went wrong during execution of the command
            # NOTE a relevant exception is already logged with traceback by
            # Application Manager
            self.__logger.error("Error received while executing command: "
                                f"{steering_command.name}")
            # terminate with ERROR
            return Response.ERROR

        # Case b. response in not an ERROR
        self.__logger.info(f"Steering command: '{steering_command.name}' is "
                           "executed successfully.")
        return Response.OK

    def __send_command_to_application_manager(self, command):
        self.__logger.debug(f'sending {command} to Application Manager.')
        # send START command to Application Manager
        self.__communicator.send(command,
                                 self.__application_manager_in_queue)

    def __get_interscalehub_proxy_list(self):
        """
        returns the InterscaleHub proxy after fetching it from Registry Service
        """
        interscalehub_proxy_list = []
        while not interscalehub_proxy_list:
            # wait until it gets InterscaleHub proxy from Registry
            self.__logger.info("__DEBUG__ waiting for InterscaleHub"
                               " connection details. retry in 1 sec")
            time.sleep(1)
            interscalehub_proxy_list =\
                self.__health_registry_manager_proxy.find_all_by_category(
                    SERVICE_COMPONENT_CATEGORY.INTERSCALE_HUB)

        # return the proxy
        return interscalehub_proxy_list

    def __execute_init_command(self):
        """helper function to execute INIT steering command"""
        self.__logger.info("Executing INIT command!")
        # 1. update local state
        self.__ac_registered_component_service = self.__update_local_state(SteeringCommands.INIT)
        if self.__ac_registered_component_service == Response.ERROR:
            # terminate loudly as state could not be updated
            # exception is already logged with traceback
            return self.__respond_with_state_update_error()

        # 2. proceed only if the action is InterscaleHub, otherwise wait until
        # the InterscaleHub registers its connections details
        #  fetch the action id
        actions_id = None
        try:
            actions_id = self.__actions.get('action-id')
        except KeyError:
            # 'action-id' could not be found in 'actions' dictionary
            # log the exception with traceback
            self.__logger.exception("'action-id' is not a valid key.")
            return Response.ERROR

        # Case a: action type is SIMULATOR
        # fetch InterscaleHub MPI endpoint connection details
        if actions_id == 'action_004' or actions_id == 'action_010':  # TODO will be updated with the actions_type from XML
            interscalehub_proxy_list = self.__get_interscalehub_proxy_list()
            interscalehub_mpi_endpoints = [interscalehub_proxy.endpoint
                                           for interscalehub_proxy in
                                           interscalehub_proxy_list]
            # append interscale_hub mpi endpoint connection details with
            # actions as parameters
            self.__actions.append(interscalehub_mpi_endpoints)

        # 2. initialize Application Manager
        self.__application_manager = ApplicationManager(
            # parameters for setting up the uniform log settings
            self._log_settings,  # log settings
            self._configurations_manager,  # Configurations Manager
            # actions (applications) to be launched
            self.__actions,
            # proxy to shared queue to send the commands to
            # Application Manager
            self.__application_manager_in_queue,
            # proxy to shared queue to receive responses from
            # Application Manager
            self.__application_manager_out_queue,
            # proxy to Health & Registry Manager
            self.__health_registry_manager_proxy,
            # flag to enable/disable resource usage monitoring
            # TODO set monitoring enable/disable settings from XML
            enable_resource_usage_monitoring=True,
        )

        # 3. start the application Manager
        self.__logger.debug("starting application Manager.")
        self.__application_manager.start()
        # send INIT command to Application Manager
        steering_command = SteeringCommands.INIT
        self.__send_command_to_application_manager(steering_command)

        # 4. wait until a response is received from Application Manager after
        # command execution

        # NOTE if action type is SIMULATOR then response is PID and the local
        # minimum step-size, Otherwise the response is PID and connection
        # endpoint details if it is INTERSCALE_HUB
        response = self.__receive_response_from_application_manager()

        # if action type is INTERSCALEHUB then register the connection details
        # with registry
        if actions_id == 'action_006' or actions_id == 'action_008':  # TODO will be updated with the actions_type
            # register endpoint with registry service
             # 2. register with registry
            if self.__health_registry_manager_proxy.register(
                    response["PID"],  # id
                    SERVICE_COMPONENT_CATEGORY.INTERSCALE_HUB,  # name
                    SERVICE_COMPONENT_CATEGORY.INTERSCALE_HUB,  # category
                    response["MPI_CONNECTION_INFO"],  # endpoint
                    None,  # current status
                    # current state
                    None
                ) == Response.ERROR:
                self.__logger.info("__DEBUG__ Could not registered INTERSCALEHUB endpoints")
                return Response.ERROR
            else:
                self.__logger.info("__DEBUG__ INTERSCALEHUB endpoints are registered")
                # InterscaleHubs do not have minimum stepsize, so send empty
                # dictionary as a response to Orchestrator
                response = {}

        # 5. send response to Orchestrator
        self.__send_response_to_orchestrator(response)
        return self.__command_execution_response(response, steering_command)

    def __execute_start_command(self):
        """helper function to execute START steering command"""
        self.__logger.info("Executing START command!")

        # 1. update local state
        self.__ac_registered_component_service = self.__update_local_state(SteeringCommands.START)
        if self.__ac_registered_component_service == Response.ERROR:
            # terminate loudly as state could not be updated
            # exception is already logged with traceback
            return self.__respond_with_state_update_error()

        # 2. start the application execution
        # send START command to Application Manager
        steering_command = SteeringCommands.START
        self.__send_command_to_application_manager(steering_command)

        # 3. wait until a response is received from Application Manager after
        # command execution
        response = self.__receive_response_from_application_manager()

        # 4. send response to Orchestrator
        self.__send_response_to_orchestrator(response)
        return self.__command_execution_response(response, steering_command)

    def __execute_end_command(self):
        """helper function to execute END steering command"""
        self.__logger.info("Executing END command!")
        # 1. update local state
        self.__ac_registered_component_service = self.__update_local_state(SteeringCommands.END)
        if self.__ac_registered_component_service == Response.ERROR:
            # terminate loudly as state could not be updated
            # exception is already logged with traceback
            return self.__respond_with_state_update_error()

        # 2. send END command to Application Manager
        steering_command = SteeringCommands.END
        self.__send_command_to_application_manager(steering_command)

        # 3. wait until a response is received from Application Manager after
        # command execution
        response = self.__communicator.receive(
                        self.__application_manager_out_queue)
        self.__logger.debug(f"response from Application Manager {response}")
        # TODO check for response and act accordingly

        # 4. send response to orchestrator
        self.__send_response_to_orchestrator(response)
        return self.__command_execution_response(response, steering_command)

    def __terminate_with_error(self):
        """helper function to terminate the execution with error."""
        self.__logger.critical("rasing signal to terminate with error.")
        # raise signal
        signal.raise_signal(signal.SIGTERM)
        # terminate with error
        return Response.ERROR

    def __handle_fatal_event(self):
        '''
        helper function to handle a FATAL event received for a pre-emptory
        termination from Orchestrator.
        '''
        self.__logger.critical("quitting forcefully!")
        # return with ERROR to indicate preemptory exit
        # NOTE an exception is logged with traceback by calling function
        # when return with ERROR
        return Response.ERROR

    def __receive_broadcast(self):
        """receives and returns the broadcasted message"""
        # broadcast is received as a multipart message
        # i.e. [subscription topic, steering command]
        topic, command = self.__subscription_endpoint_with_command_control.recv_multipart()
        # return the de-serialized command
        return pickle.loads(command)

    def __fetch_and_execute_steering_commands(self):
        """
        Main loop to fetch and execute the steering commands.
        The loop terminates either by normally or forcefully i.e.:
        i)  Normally: receveing the steering command END, or by
        ii) Forcefully: receiving the FATAL command from Orchestrator.
        """
        # create a dictionary of choices for the steering commands and
        # their corresponding executions
        command_execution_choices = {
            SteeringCommands.INIT: self.__execute_init_command,
            SteeringCommands.START: self.__execute_start_command,
            SteeringCommands.END: self.__execute_end_command,
            EVENT.FATAL: self.__handle_fatal_event,
        }

        # loop for executing and fetching the steering commands
        while True:
            # 1. fetch the Steering Command
            self.__logger.debug("waiting for steering command")
            # receive steering command from broadcast
            current_steering_command = self.__receive_broadcast()
            self.__logger.debug(f"got the command {current_steering_command.name}")
            # 2. execute the current steering command
            if command_execution_choices[current_steering_command]() ==\
                    Response.ERROR:
                # something went wrong, terminate loudly with error
                try:
                    # raise runtime exception
                    raise RuntimeError
                except RuntimeError:
                    # log the exception with traceback details
                    self.__logger.exception(
                        f"Error executing command: "
                        f"{current_steering_command.name}. "
                        f"Quiting!"
                    )
                finally:
                    return self.__terminate_with_error()

            # 3 (a). If END command is executed, finish execution as normal
            if current_steering_command == SteeringCommands.END:
                self.__logger.info("Concluding Application Companion")
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
            self.__logger.error("setup failed!.")
            return Response.ERROR

        # ii. loop for fetching and executing the steering commands
        return self.__fetch_and_execute_steering_commands()
