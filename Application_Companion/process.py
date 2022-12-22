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

from EBRAINS_RichEndpoint.Application_Companion.cpu_usage import CPUUsage
from EBRAINS_RichEndpoint.Application_Companion.memory_usage import MemoryUsage
from EBRAINS_RichEndpoint.Application_Companion.resource_usage_summary import ResourceUsageSummary
from EBRAINS_RichEndpoint.Application_Companion.common_enums import Response


class Process:
    '''
    Represents the meta-data for the process executing the application.
    This is particularly useful for later Object Relational Mapping (ORM)
    with Knowledge Graph (KG).
    '''
    def __init__(self, log_settings, configurations_manager,
                 action_process_name, pid, process_affinity, path_to_read_stats=" "
                 ):
        self._log_settings = log_settings
        self._configurations_manager = configurations_manager
        self.__logger = self._configurations_manager.load_log_configurations(
                                        name=__name__,
                                        log_configurations=self._log_settings)
        self.__logger.debug("logger is configured.")
        self.__process_id = pid
        if not path_to_read_stats:
            self._path_to_read_stats = path_to_read_stats
        else:
            self._path_to_read_stats = os.path.join(
                '/proc/', str(self.__process_id), 'stat')
        self.__process_name = action_process_name
        self.__process_starting_time =\
            self.__get_process_meta_information(
                self._path_to_read_stats)
        self.__process_execution_time = 0
        self.__process_affinity = process_affinity
        self.__cpu_usage = CPUUsage(
                        process_id=pid,
                        log_settings=self._log_settings,
                        configurations_manager=self._configurations_manager)
        self.__memory_usage = MemoryUsage(
                        process_id=pid,
                        log_settings=self._log_settings,
                        configurations_manager=self._configurations_manager)
        self.__resource_usage_summary = ResourceUsageSummary()

    @property
    def process_id(self):
        return self.__process_id

    @property
    def process_affinity(self):
        return self.__process_affinity
    
    @property
    def process_name(self):
        return self.__process_name

    @property
    def process_starting_time(self):
        return self.__process_starting_time

    @property
    def process_execution_time(self):
        return self.__process_execution_time

    @process_execution_time.setter
    # set by CPU usage monitor after completing execution
    def process_execution_time(self, execution_time):
        self.__process_execution_time = execution_time

    @property
    def per_cpu_usage_stats(self):
        stats = self.all_cpus_usage_stats.copy()
        self.__logger.debug(f"all_cpus_usage_stats:{stats}")
        if len(self.__process_affinity) == 1:
            per_cpu_avg_usage = self.all_cpus_usage_stats
        else:
            per_cpu_avg_usage =\
                list(map(lambda value: 
                     [value[0], value[1] / len(self.__process_affinity) * 100],
                     stats))
        self.__logger.debug(f"per_cpu_avg_usage:{per_cpu_avg_usage}")
        return  per_cpu_avg_usage

    @property
    def all_cpus_usage_stats(self):
        return self.__resource_usage_summary.cpu_usage_stats

    @all_cpus_usage_stats.setter
    def all_cpus_usage_stats(self, cpu_usage):
        self.__resource_usage_summary.cpu_usage_stats.append(cpu_usage)

    @property
    def memory_usage_stats(self):
        return self.__resource_usage_summary.memory_usage_stats

    @memory_usage_stats.setter
    def memory_usage_stats(self, memory_usage):
        self.__resource_usage_summary.memory_usage_stats.append(memory_usage)

    @property
    def mean_cpu_usage(self): 
        """returns the mean of total cpu usage"""
        # comput the mean from list of tuples: [(timnestamp, cpu_usage_stats)]
        avg_usage_per_cpu = self.per_cpu_usage_stats.copy()
        # mean_usage = ((avg_usage_per_cpu/ len(self.__process_affinity))*100)
        mean_usage = self.__resource_usage_summary.mean_cpu_usage(avg_usage_per_cpu)
        self.__logger.debug(f"mean_cpu_usage:{mean_usage}")
        return mean_usage
        
    @property
    def mean_memory_usage(self):
        """returns the mean of total memory usage"""
        # compute the mean from list of tuples: [(timnestamp, memory_usage_stats)]
        return self.__resource_usage_summary.mean_memory_usage

    def get_cpu_stats(self):
        """
            returns the cpu usage with time-stamp and the current exection time
            of the process.
        """
        timestamp, current_cpu_usage_stats, process_running_time = \
            self.__cpu_usage.get_usage_stats()
        self.__process_execution_time = process_running_time
        return timestamp, current_cpu_usage_stats, process_running_time

    def get_memory_stats(self):
        """returns the memory usage stats with time-stamp"""
        return self.__memory_usage.get_usage_stats()

    def __get_process_meta_information(self, proc_stat_file):
        return self.__parse(self.__read(proc_stat_file))

    def __read(self, proc_stat_file):
        try:
            with open(proc_stat_file) as stat_file:
                return next(stat_file)
        except OSError as e:
            # An exception is raised while attempting to open the file because
            # e.g. the process is already finished therefore the /proc/<pid>/stat file
            # does not exist anymore
            self.__logger.exception(f"{type(e)}: {e}")
            return Response.ERROR_READING_FILE

    def __parse(self, stat_line):
        process_name = None
        process_starting_time = None
    
        if stat_line != Response.ERROR_READING_FILE:
            self.__logger.debug(f'stat_line: {stat_line}')
            proc_pid_stats = stat_line.split(' ')
            # process_name = proc_pid_stats[1].strip('()')  # remove sorrounding parentehses
            process_starting_time = int(proc_pid_stats[21])
            self.__logger.debug(f"process_name:{process_name}, "
                                f"fprocess start time: {process_starting_time}")
        return process_starting_time
