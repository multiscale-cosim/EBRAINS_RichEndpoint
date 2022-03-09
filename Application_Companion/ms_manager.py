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
import argument_parser

from EBRAINS_ConfigManager.global_configurations_manager.xml_parsers.configurations_manager import ConfigurationsManager
from EBRAINS_RichEndpoint.launcher import Launcher


#############################################################################
#  This demonstrates the functionality by running the example applications  #
#############################################################################
if __name__ == '__main__':
    """
    Entry point to launch the orchestrator and application companions to
    execute the action plan and to demonstrate POC of steering via CLI.
    """
    configurations_manager = ConfigurationsManager()
    log_settings = configurations_manager.get_configuration_settings(
        'log_configurations', 'global_settings.xml')
    # get path to setup output directory from the XML configuration file
    default_dir = configurations_manager.get_configuration_settings(
        'output_directory', 'global_settings.xml')
    # setup default directories (Output, Output/Results, Output/Logs,
    # Output/Figures, Output/Monitoring_DATA)
    configurations_manager.setup_default_directories(default_dir['output_directory'])
    logger = configurations_manager.load_log_configurations(
                                            name=__name__,
                                            log_configurations=log_settings)
    logger.debug("logger is configured!")
    # get path to application to be executed
    logger.debug('parsing CLI args')
    __CLI_args = argument_parser.get_parsed_CLI_arguments()
    applications = [{'action': __CLI_args.app, 'args': __CLI_args.param},
                    {'action': __CLI_args.app, 'args': __CLI_args.param}]
    logger.info(f'application to be executed: {applications}')
    launcher = Launcher(log_settings, configurations_manager)
    launcher.launch(applications)

exit(0)
