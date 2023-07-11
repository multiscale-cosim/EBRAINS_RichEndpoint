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

from EBRAINS_Launcher.common.utils import proxy_manager_server_utils
from EBRAINS_Launcher.common.utils import networking_utils
from EBRAINS_Launcher.common.utils import deployment_settings_hpc
from EBRAINS_Launcher.common.utils import multiprocess_utils

from EBRAINS_ConfigManager.workflow_configurations_manager.xml_parsers.xml_tags \
    import CO_SIM_XML_CO_SIM_SERVICES_DEPLOYMENT_SRUN_OPTIONS, \
    CO_SIM_XML_CO_SIM_SERVICES_DEPLOYMENT_SETTINGS
from EBRAINS_RichEndpoint.application_companion.common_enums import Response
import EBRAINS_RichEndpoint.application_companion.application_companion as ApplicationCompanion
from EBRAINS_RichEndpoint.orchestrator.proxy_manager_server import ProxyManagerServer
import EBRAINS_RichEndpoint.orchestrator.command_control_service as CommandControlService
import EBRAINS_RichEndpoint.orchestrator.orchestrator as Orchestrator
from EBRAINS_RichEndpoint.application_companion.common_enums import SERVICE_COMPONENT_CATEGORY
import EBRAINS_RichEndpoint.steering.steering_service as SteeringService
from EBRAINS_RichEndpoint.orchestrator.proxy_manager_client import ProxyManagerClient

from EBRAINS_ConfigManager.workflow_configurations_manager.xml_parsers import constants


class LauncherHPC:
    '''
    launches the all the necessary (Modular Science) components to execute
    the workflow.
    '''
    def __init__(self, log_settings, configurations_manager,
                 proxy_manager_server_address=None,
                 communication_settings_dict=None,
                 services_deployment_dict=None,
                 is_execution_environment_hpc=False,
                 is_interactive=False,
                 is_monitoring_enabled=False):

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
        self.__latency = None  # TODO must be determine runtime
        # flag to determine the target platform for execution
        self.__is_execution_environment_hpc = is_execution_environment_hpc
        # flag whether the steering is interactive
        self.__is_interactive =  is_interactive
        # flag to determine whether resource usage monitroing is enabled
        self.__is_monitoring_enabled = is_monitoring_enabled
        # set port range for components
        if self.__communication_settings_dict is not None:
            # initialize with values parsed from XML configuration file
            self.__ports_for_command_control_channel = self.__communication_settings_dict
            self.__logger.debug("range of ports for C&C channel is initialized"
                                " with range defined in XML configurations.")
        else:
            # initializing with range from networking_utils.py script located
            # in common/utils
            self.__ports_for_command_control_channel = networking_utils.default_range_of_ports
            self.__logger.debug("range of ports for C&C channel is initialized "
                                " with range defined in networking_utils.py.")

        self.__port_range_for_orchestrator = None
        self.__port_range_for_command_control = None
        self.__port_range_for_application_companions = None
        self.__port_range_for_application_manager = None
        # initialize range of ports for each component
        self.__set_up_port_range_for_components()

        # settings objects for MS components
        self.__serialized_log_settings = None
        # Configurations Manager
        self.__serialized_configurations_manager = None
        # Connection details of Proxy Manager Server
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

        self.__serialized_is_interactive = None
        self.__serialized_is_monitoring_enabled = None

        # initialize deployment settings for components
        if self.__is_execution_environment_hpc:
            self.__cosim_slurm_nodes_mapping = None
            self.__srun_command_for_cosim = None
            self.__ms_components_deployment_settings = None
            self.__init_deployment_settings(services_deployment_dict)

        self.__logger.debug("initialized.")

    def __init_deployment_settings(self, services_deployment_dict):
        """helper function to initialize deployment settings for components"""

        if services_deployment_dict is None:
            # Case a, initialize with settings defined in
            # deployment_settings_hpc.py script located in common/utils
            self.__cosim_slurm_nodes_mapping = deployment_settings_hpc.cosim_slurm_nodes_mapping(self.__logger)
            self.__srun_command_for_cosim = deployment_settings_hpc.default_srun_command.copy()
            self.__ms_components_deployment_settings = deployment_settings_hpc.deployment_settings.copy()
            self.__logger.debug("initialized the deployment settings from utils")
        else:
            # Case b, initialize with settings defined in XML configurations

            # b.1 - Assigning current HPC node names (e.g. jsc056) to the CO_SIM_SLURM_NODE_NNN variables.
            # __original__: self.__cosim_slurm_nodes_mapping = services_deployment_dict.cosim_slurm_nodes_mapping()
            # __to_be_removed __:
            # self.__cosim_slurm_nodes_mapping = deployment_settings_hpc.cosim_slurm_nodes_mapping(self.__logger)

            # b.2 - srun options
            # __original__: self.__srun_command_for_cosim = services_deployment_dict.default_srun_command.copy()
            self.__srun_command_for_cosim = \
                services_deployment_dict[CO_SIM_XML_CO_SIM_SERVICES_DEPLOYMENT_SRUN_OPTIONS]
            # b.3 - Co-Sim components HPC nodes arrangement
            # __original__:
            # self.__ms_components_deployment_settings = services_deployment_dict.deployment_settings.copy()
            self.__ms_components_deployment_settings = \
                services_deployment_dict[CO_SIM_XML_CO_SIM_SERVICES_DEPLOYMENT_SETTINGS]
            self.__logger.debug("initialized the deployment settings from XML")

    def __set_up_port_range_for_components(self):
        '''
            initializes the range of ports for each Modular Science Component
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
        self.__logger.debug(f"nodelist: {nodelist}")
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
                stdout_line = multiprocess_utils.non_block_read(
                        self.__logger,
                        process.stdout)
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
                stderr_line = multiprocess_utils.non_block_read(
                        self.__logger,
                        process.stderr)
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

        # 3. launch Proxy Manager Server
        self.__logger.info('setting up Proxy Manager Server')

        # prepare command to deploy the Proxy Manager Server
        proxy_manager_server_command = self.__deployment_command(
            # service to be executed
            ProxyManagerServer,
            # service category name to fetch the deployment settings
            SERVICE_COMPONENT_CATEGORY.PROXY_MANAGER_SERVER.name,
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
        # has already started listening for connections
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

        connection_details = pickle.loads(
            base64.b64decode(self.__serialized_proxy_manager_connection_details))    
        self._proxy_manager_client.connect(
            connection_details["IP"],
            connection_details["PORT"],
            connection_details["KEY"])

        # 5. get the proxy to registry manager
        self.__health_registry_manager_proxy = \
            self._proxy_manager_client.get_registry_proxy()

        # all is fine
        return Response.OK

    def __deployment_command(self, service, service_component_name, *args):
        """
        helper function to get the command to deploy the service locally or
        on HPC systems.
        """
        default_cosim_nodelist_for_service = None
        if self.__is_execution_environment_hpc:
            default_cosim_nodelist_for_service = self.__ms_components_deployment_settings[
                service_component_name]
        return deployment_settings_hpc.deployment_command(
            # logger
            self.__logger,
            self.__is_execution_environment_hpc,
            inspect.getfile(service),
            # cosim default nodelist for the given service
            default_cosim_nodelist_for_service,
            # target nodelist from srun command defined in xml
            [],
            # service specific parameters
            args
            )

    def __serialize_setup_objects(self):
        """
            helper function to encode base64 and pickle the objects required to
            by MS components for initializing and setup
        """
        # Log Settings
        self.__serialized_log_settings = multiprocess_utils.b64encode_and_pickle(
            self.__logger, self._log_settings)

        # Configurations Manager
        self.__serialized_configurations_manager = multiprocess_utils.b64encode_and_pickle(
            self.__logger, self._configurations_manager)

        # Connection details of Proxy Manager Server
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

        # Boolean to indicate if steering is interactive
        self.__serialized_is_interactive = multiprocess_utils.b64encode_and_pickle(
            self.__logger, self.__is_interactive)
        
        # Boolean to indicate if steering is interactive
        self.__serialized_is_monitoring_enabled = multiprocess_utils.b64encode_and_pickle(
            self.__logger, self.__is_monitoring_enabled)

        # Boolean to indicate if target platform for deployment is HPC
        self.__serialized_is_execution_environment_hpc = multiprocess_utils.b64encode_and_pickle(
            self.__logger, self.__is_execution_environment_hpc)

    def __checkpoint_service_status(self, service):
        """
            helper function to check if the service is running and registered
            with Registry Service
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

    def __interscaelHub_num_processes(self, actions):
        self.__logger.debug(f"Number of Actions to be launched{len(actions)}")
        total_interscaleHub_num_processes = 0
        for action in actions:
            self.__logger.debug(f"action: {action}, action-goal: {action.get('action-goal')}")
            if action.get('action-goal') == constants.CO_SIM_INTERSCALE_HUB or\
               action.get('action-goal') == constants.CO_SIM_ONE_WAY_INTERSCALE_HUB:
                action_command = action.get('action')
                for index, ntasks in enumerate(action_command):
                    try:
                        if ntasks == '-n':
                            total_interscaleHub_num_processes += int(action_command[index+1])
                        elif '--ntasks=' in ntasks:
                            total_interscaleHub_num_processes += int(list(filter(str.isdigit, ntasks))[0])
                    except TypeError:
                        # list item is of type bytes 
                        self.__logger.debug(f"list item type:{type(ntasks)}")
                        # continue traversing
                        continue

        self.__logger.info(f"total_interscaleHub_num_processes: {total_interscaleHub_num_processes}")
        return int(total_interscaleHub_num_processes)
    
    def __terminate_after_cc_service_went_wrong(self, cc_service):
        '''helper funciton to terminate loudly when something wrong'''
        # shutdown proxy manager server
        self.__stop_proxy_manager_server()
        # terminate Command&Control service
        self.__terminate_launched_component(cc_service)
        # log exception with traceback and terminate with error
        self.__log_exception_and_terminate_with_error(
            f'{cc_service} is broken!')
   
    def launch(self, actions):
        '''
            launches all the necessary MS components such as Command & Control
            Service, Application Companions, Orchestrator, Steering Service.
        '''
        self.__logger.debug(f"actions to be launched: {actions}")
        # 1. setup runtime settings
        self.__setup_runtime()

        # 2. Launch Modular Science Services
        
        #####################################
        # i. Launch Command&Control service #
        #####################################
        self.__logger.info('setting up Command and Control service.')
        command_to_run_cc_service = self.__deployment_command(
                CommandControlService,
                SERVICE_COMPONENT_CATEGORY.COMMAND_AND_CONTROL.name,
                self.__serialized_log_settings,
                self.__serialized_configurations_manager,
                self.__serialized_proxy_manager_connection_details,
                self.__serialized_port_range_for_command_control)
        cc_service = subprocess.Popen(command_to_run_cc_service, shell=False)

        ###############
        # Checkpoint 1: Command&Control service is running and status is ready
        ###############
        # Case a, something went wrong with service launching
        if self.__checkpoint_service_status(
                SERVICE_COMPONENT_CATEGORY.COMMAND_AND_CONTROL) == Response.ERROR:
            # shut down Proxy Manager Server and terminate loudly
            return self.__terminate_after_cc_service_went_wrong(cc_service)

        # Case b, all is fine continue with launching
        self.__logger.info('Checkpoint: Command & Control service is '
                           'deployed successfully and status is ready!')
    # -*-*-*-*-*-*-*-*-*-*-*-*-*-*-*--*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-#
        #####################################
        # ii. Launch Application Companions #
        #####################################
        application_companions = []
        # number of Application Companions to be launched
        serialized_total_num_application_companion = multiprocess_utils.b64encode_and_pickle(
            self.__logger, len(actions))
        # number of InterscaleHub processes to be launched
        total_interscaleHub_num_processes = self.__interscaelHub_num_processes(actions)
        self.__logger.debug(f"total_interscaleHub_num_processes: {total_interscaleHub_num_processes}")
        if total_interscaleHub_num_processes < 1:
            # shut down Proxy Manager Server and terminate loudly
            return self.__terminate_after_cc_service_went_wrong(cc_service)
        
        # serialize number of interscaleHub processes
        serialized_total_interscaleHub_num_processes = multiprocess_utils.b64encode_and_pickle(
            self.__logger, total_interscaleHub_num_processes)
        
        # launch Application Companions
        for action in actions:
            # serialize the action
            self.__serialized_action = multiprocess_utils.b64encode_and_pickle(
                self.__logger, action)
            command_to_run_application_companion = self.__deployment_command(
                ApplicationCompanion,
                SERVICE_COMPONENT_CATEGORY.APPLICATION_COMPANION.name,
                self.__serialized_log_settings,
                self.__serialized_configurations_manager,
                self.__serialized_action,
                self.__serialized_proxy_manager_connection_details,
                self.__serialized_port_range_for_application_companions,
                self.__serialized_port_range_for_application_manager,
                self.__serialized_is_execution_environment_hpc,
                serialized_total_num_application_companion,
                serialized_total_interscaleHub_num_processes,
                self.__serialized_is_monitoring_enabled
            )
            self.__logger.info('setting up Application Companion.')
            application_companions.append(subprocess.Popen(
                                          command_to_run_application_companion,
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
                return self.__log_exception_and_terminate_with_error(
                    f'{application_companion} is broken!')

        # Case b, all is fine continue with launching
        self.__logger.info('Checkpoint: Application Companions are '
                           'deployed successfully and status is ready!')
    # -*-*-*-*-*-*-*-*-*-*-*-*-*-*-*--*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-#
        ############################
        # iii. Launch Orchestrator #
        ############################
        self.__logger.info('setting up Orchestrator.')
        command_to_run_orchestrator = self.__deployment_command(
            Orchestrator,
            SERVICE_COMPONENT_CATEGORY.ORCHESTRATOR.name,
            self.__serialized_log_settings,
            self.__serialized_configurations_manager,
            self.__serialized_proxy_manager_connection_details,
            self.__serialized_port_range_for_orchestrator
        )
        orchestrator = subprocess.Popen(command_to_run_orchestrator, shell=False)
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
            return self.__log_exception_and_terminate_with_error(
                f'{orchestrator} is broken!')

        # Case b, all is fine continue with launching
        self.__logger.info('Checkpoint: Orchestrator is deployed successfully '
                           'and status is ready!')
    # -*-*-*-*-*-*-*-*-*-*-*-*-*-*-*--*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-#
        ###############################
        # iv. Launch Steering Service #
        ###############################
        # TODO this POC handles both, interactive and non interactive steering
        # -> refactor/rename to represent both -> generic steering
        self.__logger.info('setting up Steering Service.')
        command_to_run_steering_service = self.__deployment_command(
            SteeringService,
            SERVICE_COMPONENT_CATEGORY.STEERING_SERVICE.name,
            self.__serialized_log_settings,
            self.__serialized_configurations_manager,
            self.__serialized_proxy_manager_connection_details,
            self.__serialized_is_communicate_via_zmqs,
            self.__serialized_is_interactive
        )
        steering_service = subprocess.Popen(command_to_run_steering_service,
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
            return self.__log_exception_and_terminate_with_error(
                f'{steering_service} is broken!')

        # Case b, all is fine continue with launching
        self.__logger.info('Checkpoint: Steering Service is deployed '
                           'successfully and status is ready!')
    # -*-*-*-*-*-*-*-*-*-*-*-*-*-*-*--*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-#

        # All MS components are deployed successfully
        self.__logger.info("All MS components are launched!")
    # -*-*-*-*-*-*-*-*-*-*-*-*-*-*-*--*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-#

        # wait until all processes are finished

        # Application Companion
        for application_companion in application_companions:
            application_companion.wait()
            self.__logger.debug(f"{application_companion} is terminated")

        # Command&Control Service
        cc_service.wait()
        self.__logger.debug(f"{application_companion} is terminated")

        # Orchestrator
        orchestrator.wait()
        self.__logger.debug(f"{application_companion} is terminated")

        # Steering Service
        steering_service.wait()
        self.__logger.debug(f"{application_companion} is terminated")

        # Proxy Manager Server
        self.__proxy_manager_server.wait()
        self.__logger.debug(f"{application_companion} is terminated")
        self.__logger.info("All MS components are terminated!")
    # -*-*-*-*-*-*-*-*-*-*-*-*-*-*-*--*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-#

        return Response.OK
