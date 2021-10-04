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
import argparse
from pathlib import Path


def get_path_to_application(path_and_appname):
    """
    Returns the path to given application if it exists at given location.
    Raises an exception if the application does not exists at specified
    location.

    Parameters
    ----------
        path_and_appname: str
            Location and application name

    Returns
    ------
        path_to_app: Path
            Path to the application if it exists.
            Otherwise, it raises ArgumentTypeError exception.
    """
    path_to_app = Path(path_and_appname)
    if path_to_app.is_file():
        return path_to_app
    else:
        raise argparse.ArgumentTypeError(f'Does not exist: <{path_to_app}>')


def add_CLI_arguments(parser):
    '''
    Fills ArgumentParser to take CLI arguments for parsing.

    Parameters
    ----------
        parser: ArgumentParser
            ArgumentParser object to fill with the program arguments
    '''
    # i. path to application
    parser.add_argument(
        '--app',
        '-a',
        help='path to the application to be executed',
        metavar='application.py',
        type=get_path_to_application,
        required=True,
    )

    # ii. application specific parameters
    parser.add_argument(
        '--param',
        '-p',
        help='application specific parameters',
        metavar='parameters',
        required=True,
    )


def get_parser():
    '''
    creates and returns an object of ArgumentParser to parse the command line
    into Python data types.
    '''
    return argparse.ArgumentParser(
                    prog='MSM',
                    usage='%(prog)s --app path --param parameters',
                    description='Manages the execution flow of an application '
                                'and monitors its resource utilization.',
                    formatter_class=argparse.RawTextHelpFormatter)


def get_parsed_CLI_arguments():
    """
    Parses the command-line arguments passed to the Modular Science Manager.

    Returns
    ------
        parsed_arguments: argparse.Namespac
            parsed arguments into Python data types
    """
    # create a parser
    parser = get_parser()
    # fill ArgumentParser to take CLI arguments for parsing
    add_CLI_arguments(parser)
    # return parsed CLI arguments
    return parser.parse_args()
