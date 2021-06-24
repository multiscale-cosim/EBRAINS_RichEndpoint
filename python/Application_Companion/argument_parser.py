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


def application_exists(path_and_appname):
    """
    Determine whether the given application exists at given location.

    Parameters
    ----------
        path_and_appname : Location and application name

    Returns
    ------
        None: when the application does not exists.
        Location path and the filename: when the application exists
        at given location.
    """
    return_value = None
    if Path(path_and_appname).is_file():
        return_value = path_and_appname
    else:
        raise argparse.ArgumentTypeError(f'{path_and_appname} is not reachable')

    return return_value


def parse_command_line_arguments(args=None):
    """
        Parses the command-line arguments passed to the
        modular science manager.
    :return:
        args
    """
    parser = argparse.ArgumentParser(
                        prog='MSM',
                        usage='%(prog)s --app path --param parameters',
                        description='Manage the execution flow of an application and monitor its resource utilzation',
                        formatter_class=argparse.RawTextHelpFormatter
                            )

    parser.add_argument(
        '--app',
        '-a',
        help='path to the application to be executed',
        metavar='application.py',
        type=application_exists,
        required=True,
    )

    parser.add_argument(
        '--param',
        '-p',
        help='application specific parameters',
        metavar='parameters',
        # type=application_exists,
        required=True,
    )

    args = parser.parse_args()

    return args
