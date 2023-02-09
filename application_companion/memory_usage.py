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
from datetime import datetime

from EBRAINS_RichEndpoint.application_companion.common_enums import Response

class MemoryUsage:
    '''
    Provides the memory usage stats for a specific process.

    NOTE: The following memory consumption details (in MiB) are supported:

    1) Size (also known as Virtual Set Size or VSS)
    2) Residual Set Size (RSS),
    3) Proportional Set Size (PSS), and
    4) Unique Set Size (USS)

    1) Size is the total accessible address space for a process. The size of
    each page allocated when backing a KernelPageSize (VMA) which is usually
    the same as the size in the page table entries [1]. It also includes the
    memory that may not be in RAM (such as although malloc allocates space but
    has not yet written).
    NOTE: VSS is rarely used to determine the real  memory usage of a
    process [2].

    2) RSS is the the amount of the mapping that is currently resident in RAM.
    But RSS can still be misleading because it only represents the size of all
    shared libraries used by the process. It does not  matter how many processes
    use the shared library, the shared  library is only loaded into memory once.
    Therefore, RSS does  not accurately reflect the memory usage of a single
    process. [1,2]

    3) PSS is different from RSS in that it uses a shared library
    proportionally. PSS is very useful data, because the PSS of all processes
    in the system are added, it just reflects the total memory occupied in the
    system. However, PSS can also be a bit misleading because when a process is
    destroyed, PSS can't accurately represent the memory returned to the
    overall system. [1,2]

    4) USS is the private memory occupied by a process. That is, the exclusive
    memory of the process. USS is very useful data because it reflects the true
    marginal cost (incremental cost) of running a particular process. When a
    process is destroyed, the USS is the memory that is actually returned to
    the system. USS is the best observation data when there is a suspicious
    memory leak in the process. [2]
    NOTE: USS can be obtained by summing Private_Clean and Private_Dir
    (i.e. Private_* entries) for every smaps entry.

    [1] https://mjmwired.net/kernel/Documentation/filesystems/proc.txt
    [2] https://www.programmersought.com/article/3842775831/
    '''
    def __init__(self, process_id, log_settings,
                 configurations_manager, path_to_read_stats=" "):
        self._log_settings = log_settings
        self._configurations_manager = configurations_manager
        self.__logger = self._configurations_manager.load_log_configurations(
                                        name=__name__,
                                        log_configurations=self._log_settings)
        self.__logger.debug("logger is configured.")
        if not path_to_read_stats:
            self._path_to_read_stats = path_to_read_stats
        else:
            # NOTE: The /proc/[pid]/smaps file is present only if the
            # CONFIG_PROC_PAGE_MONITOR kernel configuration option is enabled.
            self._path_to_read_stats = os.path.join(
                '/proc/', str(process_id), 'smaps')

        # setup memory utilization metrics
        self.__memory_metrics = ['Size', 'Rss', 'Pss',
                                 'Private_Clean', 'Private_Dirty']

    def __generate_line_that_contains(self, __memory_metrics, fp):
        # read line by line in file
        for line in fp:
            # self.__logger.info(f"__DEBUG__ running mem line:{line}")
            # check if line contains any of the metrics
            for metric in __memory_metrics:
                if metric in line:
                    # return line
                    yield line

    # TODO: move general purpose helper functions to util
    def __split_values(self, line, delimiter):
        return (map(str.strip, line.split(delimiter)))

    def __current_memory_usage(self, file):
        try:
            memory_usage = {}
            with open(file, "r") as fp:
                for line in self.__generate_line_that_contains(
                                        self.__memory_metrics, fp):
                    # split into key, value
                    key, memory_kb = self.__split_values(line, ':')
                    # strip off "kB"
                    memory_in_KB, _ = self.__split_values(memory_kb, ' ')
                    try:
                        # convert to MiB
                        memory_in_MiB = float(memory_in_KB)/1024
                        # self.__logger.info(f"__DEBUG__ current memory in MiB: {memory_in_MiB}")
                        # save the memory usage in MiB
                        memory_usage.setdefault(key, []).append(
                        memory_in_MiB)
                    except Exception:
                        # if conversion fails for some reasons, then
                        # log the exception with traceback
                        self.__logger.exception('Could not covert memory in MiB.')
                        # save the memory usage in KB
                        memory_usage.setdefault(key, []).append(
                        memory_in_KB)
                # return Response.OK
                return memory_usage
        except OSError as e:
            # An exception is raised while attempting to open the file because
            # e.g. the process is already finished therefore the
            # /proc/<pid>/smaps file does not exist anymore
            self.__logger.exception(f"{type(e)}: {e}")
            return Response.ERROR_READING_FILE

    def __sum_list_values(self, list_obj):
        return sum(list(map(float, list_obj)))

    def get_usage_stats(self):
        # Check if '/proc/<pid>/smaps' file exists
        timestamp_now = datetime.now()
        current_memory_usage = self.__current_memory_usage(
            self._path_to_read_stats)
        if current_memory_usage == Response.ERROR_READING_FILE:
            # Case a, file does not exist
            # return with error, an exception is already logged with traceback
            return Response.ERROR_READING_FILE
        
        # Case b, file exists and the stats are read
        # now fill the metrics
        memory_usage = {k: self.__sum_list_values(current_memory_usage[k])
                        for k in current_memory_usage if k in self.__memory_metrics
                        }
        try:
            self.__logger.debug(f'Memory usage: {memory_usage}')
            memory_usage['Uss'] = memory_usage['Private_Clean'] + \
                memory_usage['Private_Dirty']
        except KeyError:
            self.__logger.critical('Could not calculate Uss')
        # memory_usage = map(lambda x: x/1024, memory_usage)
        return (str(timestamp_now), memory_usage)
