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
import os
from EBRAINS_RichEndpoint.Application_Companion.underlying_platform import Platform
from EBRAINS_RichEndpoint.Application_Companion.common_enums import Response


class AffinityManager:
    """
    Facilitates to manipulate with the affinity mask for a process.
    """
    def __init__(self, log_settings, configurations_manager) -> None:
        self._log_settings = log_settings
        self._configurations_manager = configurations_manager
        self.__logger = self._configurations_manager.load_log_configurations(
                                        name=__name__,
                                        log_configurations=self._log_settings)
        self.__platfrom = Platform()
        # CPU cores available on this platform
        self.__available_cpu_cores = self.__platfrom.number_of_CPU_cores
        self.__logger.debug("Affinity Manager is initialized.")

    @property
    def available_cpu_cores(self): return self.__available_cpu_cores

    def set_affinity(self, process_id, affinity_mask):
        """
        Restrict the process with process_id as PID to a set of CPUs.

        Parameters
        ----------
        process_id : int
            process PID

        affinity_mask: list
            set of CPUs

        Returns
        ------
        int
            return code
        """
        # Case, affinity mask exceed to the available CPU cores
        if self.available_cpu_cores < len(affinity_mask):
            self.__logger.error(
                f"cannot map {affinity_mask} to the "
                f"available CPU cores: {self.available_cpu_cores}")
            return Response.ERROR

        # Otherwise, set the affinity
        try:
            os.sched_setaffinity(process_id, affinity_mask)
        except Exception:  # if affinity mask is e.g. type or value error
            # log exception with traceback details
            self.__logger.exception('Got exception when setting the affinity.')
            return Response.ERROR

        # check if affinity is set
        currently_running_on_CPUs = self.get_affinity(process_id)
        if currently_running_on_CPUs == affinity_mask:
            self.__logger.info(f"{process_id} is bound to CPU cores: "
                               f"{currently_running_on_CPUs}")
            return Response.OK

        # return with error if affinity is not set
        self.__logger.error(f"{process_id} can not be bound to "
                            f"CPU cores: {affinity_mask}")
        return Response.ERROR

    def get_affinity(self, process_id):
        """
        Returns the set of CPUs the process with process_id as PID
        is restricted to.

        Parameters
        ----------
        process_id : int
            process PID

        Returns
        ------
        list
            list of CPUs that the process is restricted to
        """
        return [*os.sched_getaffinity(process_id), ]
