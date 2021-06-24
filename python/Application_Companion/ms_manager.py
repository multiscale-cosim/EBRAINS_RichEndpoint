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
from common_enums import SteeringCommands
from python.configuration_manager.configurations_manager import ConfigurationsManager
from application_companion import ApplicationCompanion
from argument_parser import parse_command_line_arguments


def get_response(response, response_queue):
    while not response_queue.empty():
        logger.info('waiting for response')
        response.append(response_queue.get())


def send_steering_command(command_steering_queue, steering_command):
    logger.debug(f'sending {steering_command}.')
    command_steering_queue.put(steering_command)
    logger.debug(f'sent {steering_command}')


if __name__ == '__main__':
    """
    Entry point to launch the application companion
    for the main application to be executed
    and to execute the steering commands.
    """
    configurations_manager = ConfigurationsManager()
    log_settings = configurations_manager.get_configuration_settings(
        'log_configurations', 'global_settings.xml')
    logger = configurations_manager.load_log_configurations(
                                            name=__name__,
                                            log_configurations=log_settings,
                                            directory='logs',
                                            directory_path='AC results')
    logger.debug("logger is configured!")
    # get path to application to be executed
    logger.debug('parsing CLI args')
    __CLI_args = parse_command_line_arguments()
    application = {'action': __CLI_args.app, 'args': __CLI_args.param}
    logger.info(f'application to be executed: {application}')
    command_steering_queue = multiprocessing.JoinableQueue()
    application_companion_response_queue = multiprocessing.Queue()
    minimum_delays = []  # mock-up for minimum delays
    response_codes = []  # response codes from application companion

    # initialize the application companion
    application_companion = ApplicationCompanion(
                                log_settings, configurations_manager,
                                application, command_steering_queue,
                                application_companion_response_queue)
    # launch the application companion
    application_companion.start()

    ###############################################
    #  mock-up the execution of steering commands #
    ###############################################

    # TODO: get the steering commands from user interface (CLI/GUI) or from XML

    # INIT steering command
    send_steering_command(command_steering_queue, SteeringCommands.INIT.value)
    get_response(minimum_delays, application_companion_response_queue)
    logger.debug(f'minimum delay is {minimum_delays}')  # mock-up

    # START steering command
    send_steering_command(command_steering_queue, SteeringCommands.START.value)
    get_response(response_codes, application_companion_response_queue)

    # END steering command
    send_steering_command(command_steering_queue, SteeringCommands.END.value)
    get_response(response_codes, application_companion_response_queue)

    exit(0)
