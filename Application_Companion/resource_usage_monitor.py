# ------------------------------------------------------------------------------
#  Copyright 2020 Forschungszentrum Jülich GmbH and Aix-Marseille Université
# "Licensed to the Apache Software Foundation (ASF) under one or more
#  contributor license agreements; and to You under the Apache License,
#  Version 2.0. "
#
# Forschungszentrum Jülich
#  Institute: Institute for Advanced Simulation (IAS)
#    Section: Jülich Supercomputing Centre (JSC)
#   Division: High Performance Computing in Neuroscience
# Laboratory: Simulation Laboratory Neuroscience
#       Team: Multi-scale Simulation and Design
# ------------------------------------------------------------------------------
import threading
import time
import os
import errno

from EBRAINS_RichEndpoint.Application_Companion.process import Process
from EBRAINS_RichEndpoint.Application_Companion.underlying_platform import Platform
from EBRAINS_RichEndpoint.Application_Companion.common_enums import Response


class ResourceUsageMonitor:
    '''
    Monitors the resources usage by a process.
    NOTE: For now, it monitors the CPU and memory consumption details.
    However, it can be extended easily also to support monitoring of other
    resource usage.
    '''
    def __init__(self, log_settings,
                 configurations_manager,
                 pid, bind_with_cores,
                 poll_interval=1.0,  # default is 1 second
                 ):
        self._log_settings = log_settings
        self._configurations_manager = configurations_manager
        self.__logger = self._configurations_manager.load_log_configurations(
                                        name=__name__,
                                        log_configurations=self._log_settings)
        self.__logger.debug("logger is configured.")
        self._stop_event = threading.Event()
        self.__process_id = pid
        self.__cpu_usage_stats = []
        self.__memory_usage_stats = []
        self._poll_interval = poll_interval
        self.__process = Process(self._log_settings,
                                 self._configurations_manager,
                                 pid=self.process_id)
        # flag to stop monitoring
        self.keep_monitoring = True
        self.__currently_running_threads = None
        self.__cpu_usage_monitoring_done = False
        self.__memory_usage_monitoring_done = False
        self.__monitors = [
                            ('CPU usage monitor', self.get_cpu_stats),
                            ('memory usage monitor', self.get_memory_stats)
                          ]
        self.__platform = Platform()
        self.__bind_with_cores = bind_with_cores

    @property
    def memory_usage_stats(self): return self.__memory_usage_stats

    @property
    def process_id(self): return self.__process_id

    @property
    def execution_time(self): return self.__process.process_execution_time

    @property
    def cpu_usage_stats(self): return self.__cpu_usage_stats

    @property
    def keep_monitoring(self): return self.__keep_monitoring

    @keep_monitoring.setter
    def keep_monitoring(self, flag):
        self.__keep_monitoring = flag

    @property
    def process_name(self):
        return self.__process.process_name

    def __set_resource_usage_stats(self, process_exit_status):
        return {
            'Process id': self.process_id,
            'Process Name': self.process_name,
            'Affinity mask': self.__bind_with_cores,
            'Execution time [seconds]': self.execution_time,
            'Process Exit status': process_exit_status,
            'CPU usage [% average] per second': self.__process.cpu_usage_stats,
            'Memory usage [MiB] per second': self.__process.memory_usage_stats,
            'Underlying Platform Basic Details': self.__platform.basic_info,
            'CPU detailed information': self.__platform.detailed_CPUs_info
            }

    def __check_if_pid_exists(self):
            """Checks whether <pid> exists in the current process table.
            NOTE the current solution is for UNIX only.
            """
            if self.process_id < 0:
                return False
            if self.process_id == 0:
                # According to "man 2 kill" PID 0 refers to every process
                # in the process group of the calling process.
                # On certain systems 0 is a valid PID but we have no way
                # to know that in a portable fashion.
                raise ValueError('invalid PID 0')
            try:
                os.kill(self.process_id, 0)
            except OSError as e:
                if e.errno == errno.ESRCH:
                    # ESRCH == No such process
                    return False
                elif e.errno == errno.EPERM:
                    # EPERM clearly means there's a process to deny access to
                    return True
                else:
                    # According to "man 2 kill" possible error values are
                    # (EINVAL, EPERM, ESRCH)
                    raise
            else:
                return True
    
    
    def get_cpu_stats(self):
        '''
        Method for CPU usage monitoring thread
        '''
        self.__logger.info(f'starts CPU monitoring for pid:{self.process_id}')
        # keep monitoring until the process finishes
        while self.keep_monitoring:  # flag is set by Application Manager
            # check of process is not finished yet
            if self.__check_if_pid_exists() == False:
                # Case a, process is alreadt finished
                self.__logger.info(f"process with pid: {self.process_id} is already finished")
                # exit the monitoring loop
                break
            
            # Case b, process is still running
            # get CPU stats
            current_cpu_usage_stats, process_execution_time = \
                self.__process.get_cpu_stats()                
            
            # check if something went wrong while reading the stats
            if current_cpu_usage_stats == Response.ERROR_READING_FILE or\
                process_execution_time == Response.ERROR_READING_FILE:
                # Case a, error reading the CPU stats
                # NOTE a more relative exception is already logged with
                # traceback details
                self.__logger.error("Error reading cpu stats file")
                # exit the monitoring loop
                break
                
            # Case b, the stats are read successfully
            self.__process.cpu_usage_stats.append(current_cpu_usage_stats)
            time.sleep(self._poll_interval)


        # proces is finished and so is the cpu usage monitoring
        self.__process.process_execution_time = process_execution_time
        self.__cpu_usage_monitoring_done = True
        self.__logger.info(f"done with CPU monitoring for pid: "
                            f"{self.process_id}")

    def get_memory_stats(self):
        '''
        Method for memory usage monitoring thread
        '''
        self.__logger.info(f"starts memory monitoring for pid: "
                           f"{self.process_id}")
        # keep monitoring until the process finishes
        while self.keep_monitoring:  # flag is set by Application Manager
            # check of process is not finished yet
            if self.__check_if_pid_exists() == False:
                # Case a, process is alreadt finished
                self.__logger.info(f"process with pid: {self.process_id} is already finished")
                # exit the monitoring loop
                break

            # Case b, process is still running
            # get memory stats
            current_memory_usage = self.__process.get_memory_stats()
            # if current_cpu_usage_stats is negative then stop monitoring
            if current_memory_usage == Response.ERROR_READING_FILE:
                self.keep_monitoring = False
                break

            self.__process.memory_usage_stats.append(current_memory_usage)
            time.sleep(self._poll_interval)
    
        # proces is finished and so is the memory usage monitoring
        self.__memory_usage_monitoring_done = True
        self.__logger.info(f"done with memory monitoring for pid: "
                            f"{self.process_id}")
    
            

    def get_resource_usage_stats(self, process_exit_status):
        # To get the complete usage details,
        # wait until the threads finish the monitoring
        while not (self.__memory_usage_monitoring_done and
                   self.__cpu_usage_monitoring_done):
            self.__logger.debug("still monitoring!")
            continue
        return self.__set_resource_usage_stats(process_exit_status)

    def start_monitoring(self):
        '''
        Creates a monitoring thread for each monitoring job.
        This ensures the extensibility to add more resources to be monitored
        as monitoring jobs.
        '''
        for monitor_name, monitoring_target in self.__monitors:
            monitor = threading.Thread(name=monitor_name,
                                       target=monitoring_target)
            # run it non-invasively in the background without blocking the
            # Application Manager
            monitor.daemon = True
            monitor.start()
        # keep track of running threads
        # NOTE this also includes the main thread.
        self.__currently_running_threads = threading.enumerate()
        self.__logger.debug(f"currently active threads:"
                            f"{self.__currently_running_threads}")
        # test if monitoring threads are running
        if(len(self.__currently_running_threads) == threading.active_count()):
            self.__logger.debug('monitoring deamon threads started.')
            return Response.OK
        else:
            return Response.ERROR
