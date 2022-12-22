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
# ------------------------------------------------------------------------------
import time
import pickle
import base64
import subprocess
import inspect

from common.utils import proxy_manager_server_utils
from common.utils import networking_utils
from common.utils import deployment_settings_hpc
from common.utils import multiprocess_utils

from EBRAINS_RichEndpoint.Application_Companion.common_enums import Response
import EBRAINS_RichEndpoint.Application_Companion.application_companion as ApplicationCompanion
from EBRAINS_RichEndpoint.orchestrator.proxy_manager_server import ProxyManagerServer
import EBRAINS_RichEndpoint.orchestrator.command_control_service as CommandControlService
import EBRAINS_RichEndpoint.orchestrator.orchestrator as Orchestrator
from EBRAINS_RichEndpoint.Application_Companion.common_enums import Response
from EBRAINS_RichEndpoint.Application_Companion.common_enums import SERVICE_COMPONENT_CATEGORY
import EBRAINS_RichEndpoint.steering.steering_service as SteeringService
from EBRAINS_RichEndpoint.orchestrator.proxy_manager_client import ProxyManagerClient


class LauncherHPC:
    '''
    launches the all the necessary (Modular Science) components to execute
    the workflow.
    '''
    def __init__(self, log_settings, configurations_manager,
                 proxy_manager_server_address=None,
                 communication_settings_dict=None,
                 xml_deployment_settings_hpc=None):
        self._log_settings = log_settings
        self._configurations_manager = configurations_manager

        # to be used for ZMQ (or another communication framework/library)
        self.__communication_settings_dict = communication_settings_dict

        self.__logger = self._configurations_manager.load_log_configurations(
            name=__name__,
            log_configurations=self._log_settings)

        # set Proxy Manager Server connection details
        self.__proxy_manager_connection_details = {}
        self.__set_up_proxy_manager_connection_details(proxy_manager_server_address)
        self.__logger.debug("proxy_manager_server_address: "
                            f"{self.__proxy_manager_connection_details}")

        # client to Proxy Manager Server
        self._proxy_manager_client = None

        # proxy to registry manager
        self.__health_registry_manager_proxy = None
        self.__latency = None  # NOTE must be determine runtime

        # set port range for components
        if self.__communication_settings_dict is not None:
            # initialize with values parsed from XML configuration file
            self.__ports_for_command_control_channel = self.__communication_settings_dict
            self.__logger.debug("range of ports for C&C channel is intialized "
                                " with range defined in XML configurations.") 
        else:
            # initializing with range from networking_utils.py script located
            # in common/utils
            self.__ports_for_command_control_channel = networking_utils.default_range_of_ports
            self.__logger.debug("range of ports for C&C channel is intialized "
                                " with range defined in networking_utils.py.")

        self.__port_range_for_orchestrator = None
        self.__port_range_for_command_control = None
        self.__port_range_for_application_companions = None
        self.__port_range_for_application_manager = None
        # initialzie range of ports for each component
        self.__set_up_port_range_for_components()
        
        # set deployment settings for components
        if xml_deployment_settings_hpc is None:
            # Case a, initialize with settings defined in
            # deployment_settings_hpc.py script located in common/utils
            self.__cosim_slurm_nodes_mapping = deployment_settings_hpc.cosim_slurm_nodes_mapping()
            self.__srun_command_for_cosim = deployment_settings_hpc.default_srun_command.copy()
            self.__ms_components_deployment_settings = deployment_settings_hpc.deployment_settings.copy()
        else:
            # Case b, initialize with settings defined in XML configurations
            self.__cosim_slurm_nodes_mapping = xml_deployment_settings_hpc.cosim_slurm_nodes_mapping()
            self.__srun_command_for_cosim = xml_deployment_settings_hpc.default_srun_command.copy()
            self.__ms_components_deployment_settings = xml_deployment_settings_hpc.deployment_settings.copy()

        # settings objects for MS components
        self.__serialized_log_settings = None
        # Configurations Manager
        self.__serialized_configurations_manager = None
        # Conntection details of Proxy Manager Server
        self.__serialized_proxy_manager_connection_details = None
        # Range of ports for Command & Control Service
        self.__serialized_port_range_for_command_control = None
        # Range of ports for Application Companions
        self.__serialized_port_range_for_application_companions = None
        # Range of ports for Application Manager
        self.__serialized_port_range_for_application_manager = None
        # Range of ports for Orchestrator
        self.__serialized_port_range_for_orchestrator = None
        # Flag to indicate if communication is vua 0MQs
        self.__serialized_is_communicate_via_zmqs = None
        # Flag to indicate if steering is interacive
        self.__serialized_is_interactive = None

        self.__logger.debug("initialized.") 
    
    def __set_up_port_range_for_components(self):
        '''
            initialzies the range of ports for each Modular Science Component
        '''
        try:
            # Orchestrator
            self.__port_range_for_orchestrator =\
                self.__ports_for_command_control_channel["ORCHESTRATOR"]
            # Command & Control Service
            self.__port_range_for_command_control =\
                self.__ports_for_command_control_channel["COMMAND_CONTROL"]
            # Application Companion
            self.__port_range_for_application_companions =\
                self.__ports_for_command_control_channel["APPLICATION_COMPANION"]
            # Application Manager
            self.__port_range_for_application_manager =\
                self.__ports_for_command_control_channel["APPLICATION_MANAGER"]
        except KeyError:
            # log with traceback
            self.logger.exception("error reading Keys from: "
                                  f"{self.__ports_for_command_control_channel}")
            # re-raise to exit
            raise KeyError
    
    def __set_up_proxy_manager_connection_details(
                                        self, proxy_manager_server_address):
        """
            initializes Proxy Manager Server connection details
        """
        if proxy_manager_server_address is None:
            # Case a, initialize with settings defined in
            # proxy_manager_server_utils.py script located in common/utils
            self.__proxy_manager_connection_details = {
                "IP": proxy_manager_server_utils.IP,
                "PORT": proxy_manager_server_utils.PORT,
                "KEY": proxy_manager_server_utils.KEY}
        else:
             # Case b, initialize with settings defined in XML configurations
            self.__proxy_manager_connection_details = proxy_manager_server_address

    def __get_proxy_to_registered_component(self, component_service):
        '''
            It checks whether the component is registered with registry.
            If registered, it returns the proxy to component.
            Otherwise, it returns None.
        '''
        proxy = None
        while not proxy:
            # fetch proxy if component is already registered    
            proxy = self.__health_registry_manager_proxy.\
                    find_all_by_category(component_service)
            if proxy:  # Case, proxy is found
                self.__logger.debug(f'{component_service.name} is found '
                                    f'proxy: {proxy}.')
                break
            else:  # Case: proxy is not found yet
                self.__logger.debug(f"{component_service.name} is not yet "
                                   "registered, retry in 0.1 second.")
                time.sleep(0.1)  # do not hog CPU
                continue 

        # Case, proxy is found
        return proxy

    def __log_exception_and_terminate_with_error(self, error_summary):
        """
        Logs the exception with traceback and returns with ERROR as response to
        terminate with error"""
        try:
            # raise RuntimeError exception
            raise RuntimeError
        except RuntimeError:
            # log the exception with traceback
            self.__logger.exception(error_summary)
        # respond with Error to terminate
        return Response.ERROR

    def __compute_latency(self):
        """returns the latency of the network"""
        # Latency = Propagation Time + Transmission Time + Queuing Time + Processing Delay
        latency = 2  # TODO determine the latency to registry service at runtime
        return latency
    
    def __prepare_srun_command(self, service, nodelist, *args, **kwargs):
        command = []
        self.__logger.debug(f"preparing command for {service}")
        self.__logger.debug (f"nodelist: {nodelist}")
        if "--nodelist" not in nodelist:
            target_nodelist = self.__cosim_slurm_nodes_mapping[nodelist]
            command.append(f"--nodelist={target_nodelist}")
            self.__logger.debug(f"target nodelist={target_nodelist}")
        else:
            command.append(nodelist)
        self.__logger.debug(f"srun command:{command}")
        # append the service arguments required to instantiate and run it
        command.append("python3")
        command.append(f"{service}")
        for arg in args:
            command.append(arg)
        # command.append("&")
        srun_command_with_args = self.__srun_command_for_cosim + command
        self.__logger.debug(f"command with arguments:{srun_command_with_args}")
        return srun_command_with_args


    def __read_popen_pipes(self, process):
        """
        helper function to read the outputs from the process.

        Parameters
        ----------
        process : str
            process which is launched


        Returns
        ------
        int
            return code indicating whether the outputs are read
        """
        stdout_line = None  # to read from output stream
        stderr_line = None  # to read from error stream
        # read until the process is running
        while process.poll() is None:
            try:
                # read from the process output stream
                stdout_line = multiprocess_utils.non_block_read(process.stdout)
                # log the output received from the process
                if stdout_line:
                    decoded_lines = stdout_line.strip().decode('utf-8')
                    self.__logger.debug(
                        f"process <{process}>, output: {decoded_lines}")

                    # stop reading if Proxy Manager Server is started
                    # NOTE by protocol, Proxy Manager Server prints following
                    # statement when started:
                    # "starting Proxy Manager Server at..."
                    if "starting Proxy Manager Server at" in decoded_lines:
                        # server is started, start launching components
                        self.__logger.info(f"{decoded_lines}")
                        break

                # read from the process error stream
                stderr_line = multiprocess_utils.non_block_read(process.stderr)
                # log the error reported by the process
                if stderr_line:
                    self.__logger.error(
                        f"{process}: "
                        f"{stderr_line.strip().decode('utf-8')}")

                # Otherwise, wait a bit to let the process proceed the execution
                time.sleep(0.1)
                # continue reading
                continue

            # just in case if process hangs on reading
            except KeyboardInterrupt:
                # log the exception with traceback
                self.__logger.exception("KeyboardInterrupt caught by: "
                                        f"process <{process}>")
                # terminate the Popen process peremptory
                if self.__terminate_launched_component(self.__logger, process) == Response.ERROR:
                    # Case a, process could not be terminated
                    self.__logger.error('could not terminate the process '
                                        f'<{process}>')
                else:
                    # Case b, Popen process is terminated safely
                    self.__logger.critical(f'<{process}> is terminated!')
                
                # terminate reading loop with ERROR
                return Response.ERROR

        # all went well, the expected outputs are read
        return Response.OK
    
    def __setup_runtime(self):
        """
            Sets up essential settings for launching the components.
            e.g. Proxy Manager Server is essential to be started first so that
            the components could fetch the proxy to Registry Manager after
            connecting with it.

            1. Launches and starts Proxy Manager Server
            2. Connect to Proxy Manager Server
            3. Fetches the proxy to Registry Manager
        """
        # 1. determine network delay
        self.__latency = self.__compute_latency()
        
        # 2. serialize settings objects needed by MS components for setting up
        self.__serialize_setup_objects()

        # 3. laucnh Proxy Manager Server
        self.__logger.info('setting up Proxy Manager Server')
        # get deployement settings
        proxy_manager_server_deployment_settings =\
            self.__ms_components_deployment_settings[
                SERVICE_COMPONENT_CATEGORY.PROXY_MANAGER_SERVER.name]
        
        # prepare srun command for launching
        proxy_manager_server_command = deployment_settings_hpc.prepare_srun_command(
            # logger
            self.__logger,
            # path to Proxy Manager Server script
            inspect.getfile(ProxyManagerServer),
            # deployment settings for Proxy Manager Server
            proxy_manager_server_deployment_settings,
            # connection details settings for Proxy Manager Server
            self.__serialized_proxy_manager_connection_details
            )

        # start Proxy Manager Server process
        self.__proxy_manager_server = subprocess.Popen(
                proxy_manager_server_command,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                shell=False)
        
        # read outputs from Proxy Manager Server process to confirm if it
        # has already started listening for conenctions
        if self.__read_popen_pipes(self.__proxy_manager_server) == Response.ERROR:
            # Case a, server could not be started
            # log the exception with traceback and terminate with error
            return self.__log_exception_and_terminate_with_error(
                                "Could not read output!")
        # Case b, server is started
        self.__logger.info("Proxy Manager Server is started!")

        # 4. Connect with Proxy Manager Server
        # NOTE: it terminates with RuntimeError if connection could ne be made
        # for whatever reasons
        self._proxy_manager_client = ProxyManagerClient(
            self._log_settings,
            self._configurations_manager)

        connection_details = pickle.loads(base64.b64decode(self.__serialized_proxy_manager_connection_details))    
        self._proxy_manager_client.connect(
            connection_details["IP"],
            connection_details["PORT"],
            connection_details["KEY"]) 

        # 5. get the proxy to registry manager
        self.__health_registry_manager_proxy = \
            self._proxy_manager_client.get_registry_proxy()

        # all is fine
        return Response.OK
    
    def __serialize_setup_objects(self):
        """
            helper function to encode base64 and pickle objects required to
            start MS components
        """
        # Log Settings
        self.__serialized_log_settings = multiprocess_utils.b64encode_and_pickle(
            self.__logger, self._log_settings)
        
        # Configurations Manager
        self.__serialized_configurations_manager = multiprocess_utils.b64encode_and_pickle(
            self.__logger, self._configurations_manager)
        
        # Conntection details of Proxy Manager Server
        self.__serialized_proxy_manager_connection_details = multiprocess_utils.b64encode_and_pickle(
            self.__logger, self.__proxy_manager_connection_details)
        
        # Range of ports for Command & Control Service
        self.__serialized_port_range_for_command_control = multiprocess_utils.b64encode_and_pickle(
            self.__logger, self.__port_range_for_command_control)
        
        # Range of ports for Application Companions
        self.__serialized_port_range_for_application_companions = multiprocess_utils.b64encode_and_pickle(
            self.__logger, self.__port_range_for_application_companions)
        
        # Range of ports for Application Manager
        self.__serialized_port_range_for_application_manager = multiprocess_utils.b64encode_and_pickle(
            self.__logger, self.__port_range_for_application_manager)
        
        # Range of ports for Orchestrator
        self.__serialized_port_range_for_orchestrator = multiprocess_utils.b64encode_and_pickle(
            self.__logger, self.__port_range_for_orchestrator)
        
        # Boolean to indicate if communication is vua 0MQs
        self.__serialized_is_communicate_via_zmqs = multiprocess_utils.b64encode_and_pickle(
            self.__logger, True)
        
        # Boolean to indicate if steering is interacive
        self.__serialized_is_interactive = multiprocess_utils.b64encode_and_pickle(
            self.__logger, False)

    def __checkpoint_service_status(self, service):
        """
            helper function to check if the service is running and registered
            registered with Registry Service
        """
        if self.__get_proxy_to_registered_component(service) is None:
            # Case a, something went wrong and service is not registered with
            # Registry
           return Response.ERROR
        else:
            # Case b, all is fine
            return Response.OK
    
    def __terminate_launched_component(self, component):
        """terminates the launched subprocess"""
        return multiprocess_utils.stop_preemptory(self.__logger, component)
    
    def __stop_proxy_manager_server(self):
        """helper function to stop Proxy Manager Server"""
        self._proxy_manager_client.stop_server()
    
    def launch(self, actions):
        '''
            launches all the necessary MS components such as Command & Control
            Service, Application Companions, Orchestrator, Steering Service.
        '''
       
        # 1. setup runtime settings
        self.__setup_runtime()

        # 2. Launch Modular Science Services
        
        #####################################
        # i. Launch Command&Control service #
        #####################################
        self.__logger.info('setting up Command and Control service.')
        cc_service_command = self.__prepare_srun_command(
                inspect.getfile(CommandControlService),
                self.__ms_components_deployment_settings[
                    SERVICE_COMPONENT_CATEGORY.COMMAND_AND_CONTROL.name],
                self.__serialized_log_settings,
                self.__serialized_configurations_manager,
                self.__serialized_proxy_manager_connection_details,
                self.__serialized_port_range_for_command_control)
        cc_service = subprocess.Popen(cc_service_command, shell=False)

        ###############
        # Checkpoint 1: Command&Control service is running and status is ready
        ###############
        # Case a, something went wrong with service launching
        if self.__checkpoint_service_status(
            SERVICE_COMPONENT_CATEGORY.COMMAND_AND_CONTROL) == Response.ERROR:
            # shutdown proxy manager server
            self.__stop_proxy_manager_server()
            # terminate Command&Control service
            self.__terminate_launched_component(cc_service)
            # log exception with traceback and terminate with error
            self.__log_exception_and_terminate_with_error(
                f'{cc_service} is broken!')
        
        # Case b, all is fine continue with launching
        self.__logger.info('Checkpoint: Command & Control service is '
                           'deployed successfully and status is ready!')
    #-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*--*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-#
        #####################################
        # ii. Launch Application Companions #
        #####################################
        application_companions = []
        target_nodelist = None
        for action in actions:
            # serialize the action
            self.__serialized_action = multiprocess_utils.b64encode_and_pickle(
                self.__logger, action)
            application_companion_command = self.__prepare_srun_command(
                inspect.getfile(ApplicationCompanion),
                self.__ms_components_deployment_settings[
                    SERVICE_COMPONENT_CATEGORY.APPLICATION_COMPANION.name],
                self.__serialized_log_settings,
                self.__serialized_configurations_manager,
                self.__serialized_action,
                self.__serialized_proxy_manager_connection_details,
                self.__serialized_port_range_for_application_companions,
                self.__serialized_port_range_for_application_manager
            )
            self.__logger.info('setting up Application Companion.')
            application_companions.append(subprocess.Popen(
                                          application_companion_command,
                                          shell=False))
        ###############
        # Checkpoint 2: Application Companions are running and status is ready
        ###############
        is_terminated = False
        is_broken_service = False
        for application_companion in application_companions:
            #  Case a, something went wrong with service launching
            if self.__checkpoint_service_status(
                SERVICE_COMPONENT_CATEGORY.APPLICATION_COMPANION) == Response.ERROR:
                if not is_terminated:
                    # shutdown proxy manager server
                    self.__stop_proxy_manager_server()
                    # terminate Command&Control service
                    self.__terminate_launched_component(cc_service)
                    is_terminated = True
                    is_broken_service = True
                #  terminate Application Companion
                self.__terminate_launched_component(application_companion)
            
            # log exception with traceback and terminate with error
            if is_broken_service:
                self.__log_exception_and_terminate_with_error(
                    f'{application_companion} is broken!')
        
        # Case b, all is fine continue with launching
        self.__logger.info('Checkpoint: Application Companions are '
                           'deployed successfully and status is ready!')
    #-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*--*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-#
        ############################
        # iii. Launch Orchestrator #
        ############################
        self.__logger.info('setting up Orchestrator.')
        orchestrator_command = self.__prepare_srun_command(
            inspect.getfile(Orchestrator),
            self.__ms_components_deployment_settings[
                SERVICE_COMPONENT_CATEGORY.ORCHESTRATOR.name],
            self.__serialized_log_settings,
            self.__serialized_configurations_manager,
            self.__serialized_proxy_manager_connection_details,
            self.__serialized_port_range_for_orchestrator
        )
        orchestrator = subprocess.Popen(orchestrator_command, shell=False)
        ###############
        # Checkpoint 3: Orchestrator is running and status is ready
        ###############
        # Case a, something went wrong with service launching
        if self.__checkpoint_service_status(
            SERVICE_COMPONENT_CATEGORY.ORCHESTRATOR) == Response.ERROR:
            # shutdown proxy manager server
            self.__stop_proxy_manager_server()
            # terminate Command&Control service
            self.__terminate_launched_component(cc_service)
            # terminate Application Companions
            for application_companion in application_companions:
                self.__terminate_launched_component(application_companion)
            # log exception with traceback and terminate with error
            self.__log_exception_and_terminate_with_error(
                f'{orchestrator} is broken!')
        
        # Case b, all is fine continue with launching
        self.__logger.info('Checkpoint: Orchestrator is deployed successfully '
                           'and status is ready!')
    #-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*--*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-#
        ###############################
        # iv. Launch Steering Service #
        ###############################
        # TODO this POC handles both, interactive and non interactive steering
        # -> refactor/rename to represent both -> generic steering
        self.__logger.info('setting up Steering Service.')
        steering_service_command = self.__prepare_srun_command(
            inspect.getfile(SteeringService),
            self.__ms_components_deployment_settings[
                SERVICE_COMPONENT_CATEGORY.STEERING_SERVICE.name],
            self.__serialized_log_settings,
            self.__serialized_configurations_manager,
            self.__serialized_proxy_manager_connection_details,
            self.__serialized_is_communicate_via_zmqs,
            self.__serialized_is_interactive
        )
        steering_service = subprocess.Popen(steering_service_command,
                                            shell=False)
        
        # Checkpoint 4: Steering Service is running and status is ready
        # Case a, something went wrong with service launching
        if self.__checkpoint_service_status(
            SERVICE_COMPONENT_CATEGORY.STEERING_SERVICE) == Response.ERROR:
            # shutdown proxy manager server
            self.__stop_proxy_manager_server()
            # terminate Command&Control service
            self.__terminate_launched_component(cc_service)
            # terminate Application Companions
            for application_companion in application_companions:
                self.__terminate_launched_component(application_companion)
            # terminate Orchestrator
            self.__terminate_launched_component(orchestrator)
            # log exception with traceback and terminate with error
            self.__log_exception_and_terminate_with_error(
                f'{steering_service} is broken!')
        
        # Case b, all is fine continue with launching
        self.__logger.info('Checkpoint: Steering Service is deployed '
                           'successfully and status is ready!')
    #-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*--*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-#
        # All MS components are deployed successfully
        self.__logger.info("All MS components are launched!")
        return Response.OK
