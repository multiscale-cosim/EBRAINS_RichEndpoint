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
from EBRAINS_RichEndpoint.Application_Companion.common_enums import Response


class CPUUsage:
    '''
    Provides the average usage of the CPU in percentage by a specific process.
    TODO add support for CPU usage in time windows.
    '''
    def __init__(self, process_id,
                 log_settings,
                 configurations_manager,
                 path_to_read_stats=" "):
        self._log_settings = log_settings
        self._configurations_manager = configurations_manager
        self.__logger = self._configurations_manager.load_log_configurations(
                                        name=__name__,
                                        log_configurations=self._log_settings)
        self.__logger.debug("logger is configured.")
        # set user specific file for stats reading
        if not path_to_read_stats:
            self._path_to_read_stats = path_to_read_stats
        else:
            # else set /proc/[pid]/stat
            self._path_to_read_stats = os.path.join('/proc/',
                                                    str(process_id), 'stat')
        self.__user_hz = self.__get_user_hz()
        self.__path_to_system_uptime = '/proc/uptime'  # time since last reboot
        self.__process_name = None

    @property
    def process_name(self):
        return self.__process_name  # to be used by the Process object

    def __get_user_hz(self):
        # NOTE: "CPU statistics are expressed in ticks of 1/100h of a second,
        # also called "user jiffies [1]" There are USER_HZ "jiffies" per
        # second, and on x86 systems, USER_HZ is 100." so, it's a kernel
        # constant on x86 systems.
        # However, to support frequency scheduling in user space, USER_HZ
        # is now also configurable. The actual value on the system can be
        # obtained by running the command: $ getconf CLK_TCK
        # [1] https://kb.novaordis.com/index.php/Linux_General_Concepts#USER_HZ
        cmd = "SC_CLK_TCK"  # get the actual value of USER_HZ on the system
        user_hz = os.sysconf(cmd)
        self.__logger.debug(f'USER_HZ: {user_hz}')
        return user_hz

    # NOTE: This function is particularly useful in user space frequency.
    # uncomment if needed.
    # def __get_current_clock_frequency(self):
    # scaling settings.
    #     with open("/proc/cpuinfo", "r") as fp:
    #         for line in self.__generate_line_that_contains("cpu MHz", fp):
    #             # key, frequency =  map(str.strip, line.split(':'))
    #             key,frequency = self.__split_values(line, ':')
    #             self._logger.error(f'CPUINFO: {key,frequency}')
    #             return frequency

    # def __generate_line_that_contains(self, string, fp):
    #     for line in fp:
    #         if string in line:
    #             yield line

    # TODO: move general purpose helper functions to util
    def __split_values(self, line, delimiter):
        return (map(str.strip, line.split(delimiter)))

    def __get_uptime(self):
        # NOTE: '/proc/uptime' contains the following two numbers:
        # i. the uptime of the system (seconds), and
        # ii. the amount of time spent in idle process (seconds) since then.
        # split using white spaces as delimiers
        system_uptime, system_idle_process_time = self.__split_values(
            self.__read(self.__path_to_system_uptime), None)
        self.__logger.debug(f"system uptime: {system_uptime}, "
                            f"system time spent on idle processes:"
                            f"{system_idle_process_time}")
        return (float(system_uptime), float(system_idle_process_time))

    def get_usage_stats(self):
        process_execution_time = 0
        # wait until the process run time elapsed since process start time.
        while not process_execution_time:
            total_time_with_children, process_start_time = self.__get_times()
            
            # Case a, something went wrong while reading the file
            # NOTE more specific exception is already recirded with traceback
            # where it is occured 
            if total_time_with_children == Response.ERROR_READING_FILE or\
                 process_start_time == Response.ERROR_READING_FILE:
                
                # send back the error as response
                return (Response.ERROR_READING_FILE, Response.ERROR_READING_FILE)
            
            else:
            # Case b, times are read from the /proc/<pid>/stat file
                self.__logger.debug(
                        f"total time with children: {total_time_with_children}, "
                        f"process_start_time: {process_start_time}")
                system_uptime, system_idle_process_time = self.__get_uptime()
                self.__logger.debug(
                        f"system_uptime: {system_uptime}, "
                        f"system_idle_process_time: {system_idle_process_time}")
                process_execution_time = self.__get_process_running_time(
                        system_uptime, process_start_time)
                self.__logger.debug(
                        f'process_execution_time: {process_execution_time}')
            average_cpu_usage = self.__get_current_cpu_usage(
                        total_time_with_children, process_execution_time)
            self.__logger.debug(f'average cpu usage: {average_cpu_usage}')
            return (average_cpu_usage, process_execution_time)

    def __get_process_running_time(self, system_uptime, process_start_time):
        # process-running-time(seconds) = system-uptime(seconds) - (starttime / USER_HZ) [1]
        # [1]https://kb.novaordis.com/index.php/Linux_Per-Process_CPU_Runtime_Statistics#Overview
        return (system_uptime - (process_start_time / self.__user_hz))

    def __get_current_cpu_usage(self, total_time_with_children, process_running_time):
        # average_cpu-usage = (total-time-proc-plus-children / USER_HZ) / process-running-time * 100 [1,2]
        # [1]https://kb.novaordis.com/index.php/Linux_Per-Process_CPU_Runtime_Statistics#Overview
        # [2]https://stackoverflow.com/questions/16726779/how-do-i-get-the-total-cpu-usage-of-an-application-from-proc-pid-stat
        return ((total_time_with_children / self.__user_hz) / process_running_time * 100)

    def __get_times(self):
        total_time_with_children, process_start_time = self.__parse(
            self.__read(self._path_to_read_stats))
        return total_time_with_children, process_start_time

    def __read(self, _path_to_read_stats):
        try: 
            with open(_path_to_read_stats) as stat_file:
                return next(stat_file)
        except OSError as e:
            # An exception is raised while attempting to open the file because
            # e.g. the process is already finished therefore the /proc/<pid>/stat file
            # does not exist anymore
            self.__logger.exception(f"{type(e)}: {e}")
            return Response.ERROR_READING_FILE

    def __parse(self, stat_line):
        
        if stat_line is Response.ERROR_READING_FILE:
            return (Response.ERROR_READING_FILE, Response.ERROR_READING_FILE)
        else:
        
            self.__logger.debug(f'stat_line: {stat_line}')
            proc_pid_stats = stat_line.split(' ')
            if self.__process_name is None:
                self.__process_name = proc_pid_stats[3]
            # utime: time the process has been scheduled in user mode, measured in
            # clock ticks (divide by sysconf(_SC_CLK_TCK).
            utime = int(proc_pid_stats[13])
            # stime: Amount of time that this process has been scheduled in kernel
            # mode, measured in clock ticks (divide by sysconf(_SC_CLK_TCK).
            stime = int(proc_pid_stats[14])
            # cutime:Amount of time that this process's waited-for children have
            # been scheduled in user mode, measured in clock ticks (divide by
            # sysconf(_SC_CLK_TCK).
            cutime = int(proc_pid_stats[15])
            # cstime:Amount of time that this process's waited-for children have
            # been scheduled in kernel mode, measured in clock ticks (divide by
            # sysconf(_SC_CLK_TCK).
            cstime = int(proc_pid_stats[16])
            # NOTE: utime and cutime also include guest time, cguest_time, so that
            # applications that are not aware of the guest time field do not lose
            # that time from their calculations.
            process_start_time = int(proc_pid_stats[21])
            self.__logger.debug(f"utime: {utime}, stime: {stime}, "
                                f"cutime: {cutime}, cstime : {cstime}")
            self.__logger.debug(f'process start time: {process_start_time}')
            total_time_with_children = float(utime + stime + cutime + cstime)
        return (total_time_with_children, process_start_time)
