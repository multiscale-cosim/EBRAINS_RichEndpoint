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
import sys
import signal
import pickle
import time
import base64
import zmq
import inspect
import subprocess

from EBRAINS_Launcher.common.utils import networking_utils
from EBRAINS_Launcher.common.utils.security_utils import check_integrity
from EBRAINS_Launcher.common.utils import deployment_settings_hpc
from EBRAINS_Launcher.common.utils import multiprocess_utils

from EBRAINS_RichEndpoint.application_companion.application_manager import ApplicationManager
from EBRAINS_RichEndpoint.application_companion.common_enums import EVENT, INTERCOMM_TYPE, PUBLISHING_TOPIC
from EBRAINS_RichEndpoint.application_companion.common_enums import PUBLISHING_TOPIC
from EBRAINS_RichEndpoint.application_companion.common_enums import INTEGRATED_INTERSCALEHUB_APPLICATION as INTERSCALE_HUB
from EBRAINS_RichEndpoint.application_companion.common_enums import SteeringCommands, COMMANDS
from EBRAINS_RichEndpoint.application_companion.common_enums import Response
from EBRAINS_RichEndpoint.application_companion.common_enums import SERVICE_COMPONENT_CATEGORY
from EBRAINS_RichEndpoint.application_companion.common_enums import SERVICE_COMPONENT_STATUS
from EBRAINS_RichEndpoint.application_companion.affinity_manager import AffinityManager
from EBRAINS_RichEndpoint.registry_state_machine.state_enums import STATES
from EBRAINS_RichEndpoint.orchestrator.communicator_queue import CommunicatorQueue
from EBRAINS_RichEndpoint.orchestrator.communicator_zmq import CommunicatorZMQ
from EBRAINS_RichEndpoint.orchestrator.zmq_sockets import ZMQSockets
from EBRAINS_RichEndpoint.orchestrator.communication_endpoint import Endpoint
from EBRAINS_RichEndpoint.orchestrator.proxy_manager_client import ProxyManagerClient
from EBRAINS_RichEndpoint.orchestrator import utils

from EBRAINS_InterscaleHUB.Interscale_hub.interscalehub_enums import DATA_EXCHANGE_DIRECTION
from EBRAINS_ConfigManager.global_configurations_manager.xml_parsers.configurations_manager import ConfigurationsManager
from EBRAINS_ConfigManager.workflow_configurations_manager.xml_parsers import constants


class ApplicationCompanion:
    """
    It executes the integrated application as a child process
    and controls its execution flow as per the steering commands.
    """

    def __init__(self, log_settings, configurations_manager, actions,
                 proxy_manager_connection_details,
                 port_range=None,
                 port_range_for_application_manager=None,
                 is_execution_environment_hpc=False,
                 total_application_managers=0,
                 total_interscaleHub_num_processes=0,
                 is_monitoring_enabled=False):
        self._log_settings = log_settings
        self._configurations_manager = configurations_manager
        self.__logger = self._configurations_manager.load_log_configurations(
            name="Application_Companion", log_configurations=self._log_settings
        )
        # actions (applications) to be launched
        self.__actions = actions

        # get client to Proxy Manager Server
        self._proxy_manager_client = ProxyManagerClient(
            self._log_settings,
            self._configurations_manager)

        # Connect with Proxy Manager Server
        # NOTE: it terminates with RuntimeError if connection could ne be made
        # for whatever reasons
        self.__proxy_manager_connection_details = proxy_manager_connection_details
        self._proxy_manager_client.connect(
            self.__proxy_manager_connection_details["IP"],
            self.__proxy_manager_connection_details["PORT"],
            self.__proxy_manager_connection_details["KEY"],
        )

        # Now, get the proxy to registry manager
        self.__health_registry_manager_proxy =\
            self._proxy_manager_client.get_registry_proxy()
            
        # initialize AffinityManager for handling affinity settings
        self.__affinity_manager = AffinityManager(
            self._log_settings, self._configurations_manager
        )
        self.__port_range = port_range
        self.__port_range_for_application_manager = port_range_for_application_manager
        self.__is_execution_environment_hpc = is_execution_environment_hpc
        self.__total_application_managers = total_application_managers
        self.__total_interscaleHub_num_processes = total_interscaleHub_num_processes
        self.__is_monitoring_enabled = is_monitoring_enabled
        # restrict Application Companion to a single core (i.e core 1) only
        # so not to interrupt the execution of the main application
        self.__bind_to_cpu = [0]  # TODO: configure it from configurations file
        self.__req_endpoint_with_application_manager = None
        self.__application_manager_proxy_list = []
        self.__communicator = None
        self.__endpoints_address = None
        self.__ac_registered_component_service = None
        self.__application_manager = None
        self.__action_id = None
        self.__action_goal = None
        self.__action_label = None
        self.__action_pids = []
        self.__logger.debug("Application Companion is initialized")

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

    def __set_up_channel_with_app_manager(self):
        """creates communication endpoints"""
        # 1. fetch proxy to Application Manager from registry
        application_manager_proxy_list = []
        while len(application_manager_proxy_list) < self.__total_application_managers:
            # wait until it gets Application Manager proxy from Registry
            # TODO handle deadlock here
            self.__logger.debug("looking for Application Manager Proxy in Registry. "
                                "retry in 0.1 sec!")
            time.sleep(0.1)
            application_manager_proxy = self.__get_component_from_registry(
                            SERVICE_COMPONENT_CATEGORY.APPLICATION_MANAGER
                            )
            if application_manager_proxy:
                application_manager_proxy_list = (application_manager_proxy)

        # found all proxies related to SERVICE_COMPONENT_CATEGORY.APPLICATION_MANAGER
        self.__logger.debug('found all Application Manager Proxies: '
                            f'{application_manager_proxy_list}')
        # get proxy to Application Manager belong to current action
        for proxy in application_manager_proxy_list:
            self.__logger.debug(f'running proxy: {proxy}, '
                                f'action label: {self.__action_label}')
            if self.__action_label in proxy.name:
                self.__application_manager_proxy_list.append(proxy)
                self.__logger.debug('found Application Manager Proxy: '
                    f'{self.__application_manager_proxy_list}')

        # 2. fetch Application Manager endpoint set to communicate with
        # Applicaiton Companion

        # Case a: communicate using Shared Queues
        # NOTE the functionality using queues is not tested yet and may be broken
        # if the range of ports are not provided then use the shared queues
        # assuming that it is to be deployed on laptop/single node
        if self.__port_range_for_application_manager is None:
            # get proxies to the shared queues for comunicating the commands to
            # Application Manager
            self.__application_manager_in_queue, 
            self.__application_manager_out_queue =\
            self.__application_manager_proxy_list[0].endpoint[
                SERVICE_COMPONENT_CATEGORY.APPLICATION_COMPANION]
            return Response.OK
       
       # Case: communicate using 0MQs
        else:
            self.__logger.debug("creating 0MQ endpoint")
            # Endpoint with Application Manager for communicating via a REQ socket
            self.__req_endpoint_with_application_manager =\
                self.__zmq_sockets.create_socket(zmq.REQ)

            application_manager_endpoint =\
                self.__application_manager_proxy_list[0].endpoint[
                    SERVICE_COMPONENT_CATEGORY.APPLICATION_COMPANION]
            # 3. connect with endpoint (REP socket) of Application Manager
            self.__req_endpoint_with_application_manager.connect(
                f"tcp://"  # protocol
                f"{application_manager_endpoint.IP}:"  # ip
                f"{application_manager_endpoint.port}"  # port
                )
            self.__logger.info(
                "C&C channel - connected with Application Manager at "
                f"{application_manager_endpoint.IP}"
                f":{application_manager_endpoint.port}")

            return Response.OK
    
    def __setup_command_control_channel(self):
        """creates communication endpoints"""
        # if the range of ports are not provided then use the shared queues
        # assuming that it is to be deployed on laptop/single node
        if self.__port_range is None:
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
            # # create ZMQ endpoints
            # zmq_sockets = ZMQSockets(self._log_settings, self._configurations_manager)
            # Endpoint with C&C service for receiving commands via a SUB socket
            self.__subscription_endpoint_with_command_control =\
                self.__zmq_sockets.create_socket(zmq.SUB)
            # connect with endpoint (PUB socket) of C&C service
            # fetch C&C endpoint <ip:port>
            command_and_steering_service_endpoint =\
                self.__get_command_control_endpoint()
            # subscribe to topic
            self.__zmq_sockets.subscribe_to_topic(
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
            self.__push_endpoint_with_command_control =\
                self.__zmq_sockets.create_socket(zmq.PUSH)
            self.__my_ip = networking_utils.my_ip()  # get IP address
            # bind PUSH socket to first available port in range for sending the
            # response, and get port bound to PUSH socket to communicate with
            # C&C service
            port_to_push_to_command_control =\
                self.__zmq_sockets.bind_to_first_available_port(
                    zmq_socket=self.__push_endpoint_with_command_control,
                    ip=self.__my_ip,
                    min_port=self.__port_range['MIN'],
                    max_port=self.__port_range['MAX'],
                    max_tries=self.__port_range['MAX_TRIES']
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

    def __launch_application_manager(self):
        """
            launches Application Manager on the target node(s) with required
            parameters to start executing action
        """
        # 1. encode and pickle arguments to Application Manager
        log_settings = multiprocess_utils.b64encode_and_pickle(self.__logger, self._log_settings)
        configurations_manager = multiprocess_utils.b64encode_and_pickle(self.__logger, self._configurations_manager)
        action = multiprocess_utils.b64encode_and_pickle(self.__logger, self.__actions)
        proxy_manager_connection_details = multiprocess_utils.b64encode_and_pickle(self.__logger, self.__proxy_manager_connection_details)
        port_range_for_application_manager = multiprocess_utils.b64encode_and_pickle(self.__logger, self.__port_range_for_application_manager)
        is_resource_usage_monitoring_enabled = multiprocess_utils.b64encode_and_pickle(self.__logger, self.__is_monitoring_enabled)
        is_execution_environment_hpc = multiprocess_utils.b64encode_and_pickle(self.__logger, self.__is_execution_environment_hpc)

        # append whether resource usage monitoring is enabled
        self.__actions['action'].append(is_resource_usage_monitoring_enabled)
        # NOTE Application Manager should be deployed on the nodes
        # where the action is going to be executed
        # 2. get target nodelist of action for deploying the Application Manager
        target_nodelist = None
        if self.__is_execution_environment_hpc:
            action_popen_args = self.__actions['action'].copy()
            substring = "nodelist"
            for arg in action_popen_args:
                if substring in arg:
                    target_nodelist = arg
                    break
            self.__logger.info(f"target_nodelist: {target_nodelist}")

        # 3. set arguments for Application Manager
        args_for_application_manager = [
            # parameters for setting up the uniform log settings
            log_settings,  # log settings
            configurations_manager,  # Configurations Manager
            # actions (applications) to be launched
            action,
            # connection details of Registry Proxy Manager
            proxy_manager_connection_details,
            # range of ports for Application Manager
            port_range_for_application_manager,
            # flag to enable/disable resource usage monitoring
            is_resource_usage_monitoring_enabled,
            # flag to indicate if the target environment is HPC
            is_execution_environment_hpc,]

        # 4. prepare srun command
        command_to_run_application_manager = deployment_settings_hpc.deployment_command(
            # logger
            self.__logger,
            # flag to determine target deployment platform
            self.__is_execution_environment_hpc,
            # path to Service component script to be executed
            inspect.getfile(ApplicationManager),
            # Cosim default nodelist for Application Manager
            None,
            # target nodelist in srun command for Application Manager
            target_nodelist,
            # application specific arguments
            args_for_application_manager
            )
        self.__logger.debug(f"deployment command: "
                            f"{command_to_run_application_manager}")
        # 5. launch Application Manager
        self.__application_manager = subprocess.Popen(
            command_to_run_application_manager, shell=False)

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
        
        # 2. fetch the action id
        if self.__get_action_ids() == Response.ERROR:
            # NOTE a relevant exception is already logged with traceback
            # return with error to terminate loudly
            return Response.ERROR
        
        # create ZMQ endpoints
        self.__zmq_sockets = ZMQSockets(self._log_settings, self._configurations_manager)
        
        # 3. initialize Application Manager
        self.__launch_application_manager()
        # wait a bit to let the Application Manager setup and register with registry
        time.sleep(0.1)
        # setup channel with Application Manager
        self.__set_up_channel_with_app_manager()
        
        # 1. setup communication endpoints with C&C Service
        if self.__setup_command_control_channel() == Response.ERROR:
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
                # self.__actions["action"],  # name
                self.__action_label,  # name
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

        # 5. setup communicators for Command&Control and Application Manager
        self.__setup_communicators()
        return Response.OK
    
    def __setup_communicators(self):
        """helper function to set up communicators"""
        # initialize the Communicator object for communication via Queues with
        # Application Manager
        if self.__port_range_for_application_manager:
            # initialize the Communicator object for communication via ZMQ with
            # Command&Control service
            self.__communicator = CommunicatorZMQ(
                self._log_settings,
                self._configurations_manager)

        else:
            self.__communicator = CommunicatorQueue(
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
        return self.__communicator.send(response, self.__push_endpoint_with_command_control)

    def __receive_response_from_application_manager(self):
        '''helper function to receive responses from Application Manager'''
        # response = self.__communicator.receive(
        #                 self.__application_manager_out_queue)
        response = self.__communicator.receive(
            self.__req_endpoint_with_application_manager)
        self.__logger.debug(f"response from Application Manager {response}")
        return response

    def __command_execution_response(self, response, steering_command):
        '''
        helper function to check the response received from Application Manager
        as a response. It could be for example OK, ERROR, the local minimum
        stepsize of the simulator, or connection endpoint details of
        InterscaleHubs.

        Parameters
        ----------
        response : Any
            response received from the Application Manager.

        steering_command: STEERING_COMMAND.Enum
            currently executing steering command to which the response is
            received
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
        '''helper function to send command to Application Manager'''
        self.__logger.debug(f'sending {command} to Application Manager.')
        # self.__communicator.send(command,
        #                          self.__application_manager_in_queue)
        self.__communicator.send(command,
                                 self.__req_endpoint_with_application_manager)

    def __get_interscalehub_proxy_list(self):
        """
        returns the list of proxies to InterscaleHubs after fetching it from
        Registry Service
        """
        interscalehub_proxy_list = []
        # NOTE it waits until it receives the endpoints from all InterscaleHubs
        while len(interscalehub_proxy_list) < self.__total_interscaleHub_num_processes:
            # wait until it gets InterscaleHub proxy from Registry
            # TODO handle deadlock here
            self.__logger.debug("waiting for InterscaleHub connection details. "
                               "retry in 0.1 sec!")
            time.sleep(0.1)
            interscalehub_proxy_list =\
                self.__health_registry_manager_proxy.find_all_by_category(
                    SERVICE_COMPONENT_CATEGORY.INTERSCALE_HUB)

        # return the list of proxies
        return interscalehub_proxy_list

    def __get_endpoints_as_per_simulator(self, endpoints, direction,
                                         intercomm_type):
        """
        helper function to fetch the IntersclaeHub endpoints according to
        simulator e.g. NEST_TO_TVB InterscaleHub endpoint for NEST simulator is
        where the former receives data.
        """
        for endpoint in endpoints:
            if endpoint[INTERSCALE_HUB.DATA_EXCHANGE_DIRECTION.name] == direction and\
                    endpoint[INTERSCALE_HUB.INTERCOMM_TYPE.name] == intercomm_type:
                self.__logger.debug(f"endpoint:{endpoint}")
                return endpoint

        # return None if no matching endpoint is found
        return None

    def __get_endpoints(self, simulator):
        """returns MPI endpoints of InterscaleHubs according to Simulator"""
        # intercomms = [INTERCOMM_TYPE.RECEIVER.name, INTERCOMM_TYPE.SENDER.name]
        interscaleHubs = [DATA_EXCHANGE_DIRECTION.NEST_TO_TVB.name,
                          DATA_EXCHANGE_DIRECTION.TVB_TO_NEST.name,
                          DATA_EXCHANGE_DIRECTION.NEST_TO_LFPY.name
                          ]
        # Case a: One-way data exchange
        if self.__action_goal == constants.CO_SIM_ONE_WAY_SIMULATION:
            interscaleHubs = [DATA_EXCHANGE_DIRECTION.NEST_TO_LFPY.name]
            intercomms = [INTERCOMM_TYPE.RECEIVER.name]
            self.__logger.debug(f"interscaleHubs: {interscaleHubs}, intercomm: {intercomms} ")
        
        # Case b: Two-way data exchange
        elif self.__action_goal == constants.CO_SIM_SIMULATION:
            interscaleHubs = [DATA_EXCHANGE_DIRECTION.NEST_TO_TVB.name,
                             DATA_EXCHANGE_DIRECTION.TVB_TO_NEST.name
                             ]
            intercomms = [INTERCOMM_TYPE.RECEIVER.name, INTERCOMM_TYPE.SENDER.name]
            if "TVB" in simulator:
                intercomms.reverse()
            self.__logger.debug(f"simulator: {simulator}, interscaleHubs: {interscaleHubs}, intercomm: {intercomms} ")

        # get proxies to interscalehubs
        # NOTE it waits until it receives the endpoints from all InterscaleHubs
        interscalehub_proxy_list = self.__get_interscalehub_proxy_list()
        # get list of interscalehub endpoints        
        interscalehub_endpoints_list = [interscalehub_proxy.endpoint
                                        for interscalehub_proxy in
                                        interscalehub_proxy_list]
        self.__logger.debug(f"interscalehub_endpoints_list: {interscalehub_endpoints_list} ")
        # get endpoints list as per simulator
        endpoints = []
        if len(interscaleHubs) == 1:
            for intercomm in intercomms:
                self.__logger.debug(f"interscaleHubs: {interscaleHubs}, intercomm: {intercomm} ")
                endpoint = self.__get_endpoints_as_per_simulator(
                interscalehub_endpoints_list,
                interscaleHubs[0],
                intercomm)

                # terminate with error if endpoint could not be found
                if endpoint is None:
                    self.__logger.critical("could not found endpoints, "
                                        f"simulator: {simulator}, "
                                        f"InterscaleHub:{interscaleHubs[index]}"
                                        f", intercomm: {intercomm}")
                    self.__terminate_with_error()
                endpoints.append(endpoint)
        else:
            for index, intercomm in enumerate(intercomms):
                endpoint = self.__get_endpoints_as_per_simulator(
                    interscalehub_endpoints_list,
                    interscaleHubs[index],
                    intercomm)
                # terminate with error if endpoint could not be found
                if endpoint is None:
                    self.__logger.critical("could not found endpoints, "
                                        f"simulator: {simulator}, "
                                        f"InterscaleHub:{interscaleHubs[index]}"
                                        f", intercomm: {intercomm}")
                    self.__terminate_with_error()

                # else, append endpoint to list
                endpoints.append(endpoint)
                # remove the found one endpoint to reduce the search space
                # interscalehub_endpoints_list.remove(endpoint)

        self.__logger.info(f"simulator: {simulator}, InterscaleHub endpoints: {endpoints}")
        return endpoints

    def __register_interscalehubs_endpoints(self, endpoints):
        '''helper function to register interscalehub endpoint with Registry'''
        for endpoint in endpoints:
            self.__logger.debug(f"running endpoint in response: {endpoint}")
            pid = endpoint.pop(INTERSCALE_HUB.PID.name, None)
            name = endpoint.get(INTERSCALE_HUB.DATA_EXCHANGE_DIRECTION.name, None)
            self.__logger.debug(f"running endpoint after pop: {endpoint}, "
                                f"pid: {pid}, name: {name}")
            self.__action_pids.append(pid)
            if self.__health_registry_manager_proxy.register(
                    pid,  # id
                    name,  # name
                    SERVICE_COMPONENT_CATEGORY.INTERSCALE_HUB,  # category
                    endpoint,  # endpoint
                    SERVICE_COMPONENT_STATUS.UP,  # current status
                    # current state
                    None  # NOTE Interscale-Hubs do not have states
                    ) == Response.ERROR:
                self.__logger.error("Could not registered INTERSCALEHUB "
                                    f"endpoint: {endpoint}")
                return Response.ERROR
            else:
                self.__logger.info(f"INTERSCALEHUB endpoint {endpoint} is "
                                    "registered")
                # continue registering the remaining endpoints
                continue

        # All InterscaleHub endpoints are registered successfully
        return Response.OK

    def __get_action_ids(self):
        '''
            helper function to retrieve action specific idetntifiers from
            dictionary 'actions'
        '''
        try:
            self.__action_id = self.__actions['action-id']
            self.__action_goal = self.__actions['action-goal']
            self.__action_label = self.__actions['action-label']
        except KeyError:
            # specified key could not be found in 'actions' dictionary
            # log the exception with traceback
            self.__logger.exception("not a valid key!")
            return Response.ERROR

        # action ids are retrieved
        return Response.OK
    
    def __execute_init_command(self, control_command):
        """helper function to execute INIT steering command"""
        self.__logger.info("Executing INIT command")
        # 1. update local state
        self.__ac_registered_component_service =\
            self.__update_local_state(SteeringCommands.INIT)

        if self.__ac_registered_component_service == Response.ERROR:
            # terminate loudly as state could not be updated
            # exception is already logged with traceback
            return self.__respond_with_state_update_error()

        # 2. update local state of Application Manager
        # NOTE Application Compnion updates the local states of Application  
        # Manager for transiting from STATES.READY to STATES.SYNCHRONIZATION
        # while they are waiting to receive the connection details of
        # InterscaleHubs

        self.__application_manager_proxy_list[0] =\
            self.__health_registry_manager_proxy.update_local_state(
                self.__application_manager_proxy_list[0], SteeringCommands.INIT)

        if self.__application_manager_proxy_list[0] == Response.ERROR:
            # terminate loudly as state could not be updated
            # exception is already logged with traceback
            return self.__respond_with_state_update_error()

        # 3. proceed only if the action is InterscaleHub, otherwise wait until
        # the InterscaleHub registers its connections details
        # Case a: action type is SIMULATOR
        # fetch and append InterscaleHub MPI endpoint connection details
        if self.__action_goal == constants.CO_SIM_SIMULATION or self.__action_goal == constants.CO_SIM_ONE_WAY_SIMULATION:
            # action_simulators_names = {'action_004': "NEST", 'action_010':"TVB"}
            action_with_parameters = self.__actions['action']
            self.__logger.debug(f"action_with_parameters: {action_with_parameters}")
            # get mpi endpoint connection details
            interscalehub_mpi_endpoints = self.__get_endpoints(self.__action_label)
            # append interscale_hub mpi endpoint connection details with
            # actions as parameters
            action_with_parameters.append(
                multiprocess_utils.b64encode_and_pickle(self.__logger, interscalehub_mpi_endpoints))
            
            self.__actions['action'] = action_with_parameters

        # 4. send INIT command to Application Manager
        steering_command = SteeringCommands.INIT
        # update control command with action parameters
        control_command.update_paramters(self.__actions)
        self.__send_command_to_application_manager(
            multiprocess_utils.b64encode_and_pickle(self.__logger, control_command))

        # 5. wait until a response is received from Application Manager after
        # command execution

        # NOTE if action type is SIMULATOR then response is PID and the local
        # minimum step-size, Otherwise the response is PID and connection
        # endpoint details if it is a INTERSCALE_HUB
        response = self.__receive_response_from_application_manager()

        # 6. if action type is INTERSCALEHUB then register the connection
        # details with registry
        if self.__action_goal == constants.CO_SIM_INTERSCALE_HUB or self.__action_goal == constants.CO_SIM_ONE_WAY_INTERSCALE_HUB:
            # register endpoints with registry service
            if self.__register_interscalehubs_endpoints(response) == Response.ERROR:
                # Case a: InterscaleHubs endpoints could not be registered
                # return with Error to terminate loudly
                return Response.ERROR

            # Case b: InterscaleHubs endpoints are registered
            self.__logger.debug("Interscalehub endpoints are registered")
            # InterscaleHubs do not have minimum stepsize, so send an empty
            # dictionary as a response to Orchestrator
            response = {}

        # 6. send response to Orchestrator
        self.__send_response_to_orchestrator(response)
        return self.__command_execution_response(response, steering_command)

    def __execute_start_command(self, control_command):
        """helper function to execute START steering command"""
        steering_command, _ = control_command.parse()
        self.__logger.info("Executing START command")

        # 1. update local state
        self.__ac_registered_component_service = self.__update_local_state(SteeringCommands.START)
        if self.__ac_registered_component_service == Response.ERROR:
            # terminate loudly as state could not be updated
            # exception is already logged with traceback
            return self.__respond_with_state_update_error()

        # 2. start the application execution
        # send START command with global minimum step size
        self.__send_command_to_application_manager(
            multiprocess_utils.b64encode_and_pickle(self.__logger, control_command))

        # 3. wait until a response is received from Application Manager after
        # command execution
        response = self.__receive_response_from_application_manager()

        # 4. send response to Orchestrator
        self.__send_response_to_orchestrator(response)
        return self.__command_execution_response(response, steering_command)

    def __execute_end_command(self, control_command):
        """helper function to execute END steering command"""
        steering_command, _ = control_command.parse()
        self.__logger.info("Executing END command")
        # 1. update local state
        self.__ac_registered_component_service = self.__update_local_state(SteeringCommands.END)
        if self.__ac_registered_component_service == Response.ERROR:
            # terminate loudly as state could not be updated
            # exception is already logged with traceback
            return self.__respond_with_state_update_error()

        # 2. send END command to Application Manager
        self.__send_command_to_application_manager(
            multiprocess_utils.b64encode_and_pickle(self.__logger, control_command))

        # 3. wait until a response is received from Application Manager after
        # command execution
        # response = self.__communicator.receive(
        #                 self.__application_manager_out_queue)
        response = self.__communicator.receive(
            self.__req_endpoint_with_application_manager)
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
            # receive Control Command object from broadcast
            command = self.__receive_broadcast()
            # parse the command to get the ControlCommand object and the
            # current steering command
            control_command, current_steering_command, _ = utils.parse_command(
                self.__logger, command)
            # 2. execute the current steering command
            if command_execution_choices[current_steering_command](control_command) ==\
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
        self.__logger.info("running at hostname: "
                           f"{networking_utils.my_host_name()}, "
                           f"ip: {networking_utils.my_ip()}")
        # i. setup the necessary settings for runtime such as
        # to register with registry, etc.
        if self.__set_up_runtime() is Response.ERROR:
            self.__logger.error("setup failed!.")
            return Response.ERROR

        # ii. loop for fetching and executing the steering commands
        return self.__fetch_and_execute_steering_commands()

if __name__ == '__main__':
    if len(sys.argv)==11:
        # TODO better handling of arguments parsing
        
        # 1. unpickle objects
        # unpickle log_settings
        log_settings = pickle.loads(base64.b64decode(sys.argv[1]))
        # unpickle configurations_manager object
        configurations_manager = pickle.loads(base64.b64decode(sys.argv[2]))
        # get actions (applications) to be launched
        action = pickle.loads(base64.b64decode(sys.argv[3]))
        # unpickle connection details of Registry Proxy Manager object
        proxy_manager_connection_details = pickle.loads(base64.b64decode(sys.argv[4]))
        # unpickle range of ports for Application Companion
        port_range_for_application_companions = pickle.loads(base64.b64decode(sys.argv[5]))
        # unpickle range of ports for Application Manager
        port_range_for_application_manager = pickle.loads(base64.b64decode(sys.argv[6]))
        # unpickle the flag indicating if target platform for deployment is HPC
        is_execution_environment_hpc = pickle.loads(base64.b64decode(sys.argv[7]))
        # unpickle the number of Application Manager launched
        total_application_managers = pickle.loads(base64.b64decode(sys.argv[8]))
        # unpickle the number of Application InterscaleHubs launched
        total_interscaleHub_num_processes = pickle.loads(base64.b64decode(sys.argv[9]))
        # unpickle the flag indicating if resource usage monitoring is enabled
        is_monitoring_enabled = pickle.loads(base64.b64decode(sys.argv[10]))
        
        # 2. security check of pickled objects
        # it raises an exception, if the integrity is compromised
        try:
            check_integrity(configurations_manager, ConfigurationsManager)
            check_integrity(log_settings, dict)
            check_integrity(action, dict)
            check_integrity(proxy_manager_connection_details, dict)
            check_integrity(port_range_for_application_companions, dict)
            check_integrity(port_range_for_application_manager, dict)
            check_integrity(is_execution_environment_hpc, bool)
            check_integrity(total_application_managers, int)
            check_integrity(total_interscaleHub_num_processes, int)
            check_integrity(is_monitoring_enabled, bool)
        except Exception as e:
            # NOTE an exception is already raised with context when checking
            # the integrity
            print(f'pickled object is not an instance of expected type!. {e}')
            sys.exit(1)

        # 3. all is well, instantiate Application Companion
        application_companion = ApplicationCompanion(
                                        log_settings,
                                        configurations_manager,
                                        action,
                                        proxy_manager_connection_details,
                                        port_range_for_application_companions,
                                        port_range_for_application_manager,
                                        is_execution_environment_hpc,
                                        total_application_managers,
                                        total_interscaleHub_num_processes,
                                        is_monitoring_enabled)
        # 4. start executing Application Companion
        print("launching Application Companion")
        application_companion.run()
        sys.exit(0)
    else:
        print(f'missing argument[s]; required: 11, received: {len(sys.argv)}')
        print(f'Argument list received: {str(sys.argv)}')
        sys.exit(1)
    