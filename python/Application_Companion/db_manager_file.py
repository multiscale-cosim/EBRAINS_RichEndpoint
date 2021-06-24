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
import json
from db_manager_base import DBManagerBaseClass
from common_enums import Response


class DBManagerFile(DBManagerBaseClass):
    '''
    Implements the DBManagerBaseClass for writing the data to files.
    NOTE: Later, the functionality to be extended for all CRUD commands
    to support the data loading and manipulation.
    '''
    def __init__(self, log_settings, configurations_manager) -> None:
        self._log_settings = log_settings
        self._configurations_manager = configurations_manager
        self.__logger = self._configurations_manager.load_log_configurations(
                                        name=__name__,
                                        log_configurations=self._log_settings,
                                        directory='logs',
                                        directory_path='AC results')
        self.__logger.debug('DB Manager is initialized!')

    def write(self, file, data) -> int:
        with open(file, "w") as data_file:
            json.dump(data, data_file, indent=4)
            self.__logger.info(f'data is written to file: {file}!')
            return Response.ERROR
