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
import os
import subprocess
import time
from resource_usage_monitor import ResourceUsageMonitor
from common_enums import Response
from db_manager_file import DBManagerFile
from affinity_manager import AffinityManager


class ApplicationManager(multiprocessing.Process):
    """
    Executes the application as a child process and monitors its resource usage
    """
    def __init__(self, log_settings, configurations_manager, actions,
                 am_response_queue, stop_event, kill_event,
                 enable_resource_usage_monitoring=True):
        multiprocessing.Process.__init__(self)
        self._log_settings = log_settings
        self._configurations_manager = configurations_manager
        self.__logger = self._configurations_manager.load_log_configurations(
                                        name=__name__,
                                        log_configurations=self._log_settings,
                                        directory='logs',
                                        directory_path='AC results')
        self.__logger.debug("logger is configured.")
        self.__application_manager_response_queue = am_response_queue
        self.__actions = actions
        self.__stop_event = stop_event
        self.__kill_event = kill_event
        self.__db_manager_file = DBManagerFile(
            self._log_settings, self._configurations_manager)
        self.__affinity_manager = AffinityManager(
            self._log_settings, self._configurations_manager)
        # bind the application manager to single core (i.e core 1) only
        self.__bind_to_cpu = [0]  # TODO: configure it from configurations file
        self.__process_id = os.getpid()
        self.__affinity_manager.set_affinity(self.__process_id,
                                             self.__bind_to_cpu)
        self.__legitimate_cpu_cores = self.__affinity_manager.available_cpu_cores
        self.__is_monitoring_enabled = enable_resource_usage_monitoring
        self.__logger.debug("Application Manager is initialized.")

    def run(self):
        """
        represents the application manager's main activity.
        executes the application and monitors its resource usage.
        """
        # fetch the application to be executed
        application = self.__actions.get('action')
        # fetch the application parameters
        application_args_list = self.__actions.get('args')
        try:
            # Turn-off output buffering for the child process
            os.environ['PYTHONUNBUFFERED'] = "1"
            self.__logger.debug(f'application:{application},\
                            parameters:{application_args_list}')
            # run the application
            popen_process = subprocess.Popen(
                                    [application, application_args_list],
                                    stdin=None,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE,
                                    shell=False
                                    )
            popen_process_pid = popen_process.pid
            # bind the application to set of CPUs
            # NOTE: core 1 is bound to the application manager,
            #  rest are bound to the application.
            bind_with_cores = list(range(self.__bind_to_cpu[0]+1,
                                         self.__legitimate_cpu_cores+1))
            self.__affinity_manager.set_affinity(popen_process_pid,
                                                 bind_with_cores)
            # resource usage monitoring
            if self.__is_monitoring_enabled:
                resource_usage_monitor = ResourceUsageMonitor(
                        log_settings=self._log_settings,
                        configurations_manager=self._configurations_manager,
                        pid=popen_process_pid,
                        bind_with_cores=self.__affinity_manager.get_affinity(
                            popen_process_pid))
                resource_usage_monitor.start_monitoring()
                self.__logger.debug("started monitoring the resource usage.")

        except OSError as os_error_e:
            self.__logger.error(f'Application {application} could not be run.')
            self.__logger.error(f'OSError: {os_error_e.strerror}')
            self.__application_manager_response_queue.put(Response.ERROR)
            return Response.ERROR
        except ValueError:
            self.__logger.error(f"Application {application} reports "
                                f"Popen arguments error")
            self.__logger.error(f'ValueError: {application_args_list}')
            self.__application_manager_response_queue.put(Response.ERROR)
            return Response.ERROR
        else:
            self.__logger.info(f"Application <{application}>"
                               f"is started execution successfully.")
            self.__logger.debug(f"PPID={os.getpid()},PID={popen_process_pid} "
                                f" is running Application <{application}>")
        poll_rc = None
        stdout_line = None
        stderr_line = None
        # get the outputs until the application is finished
        while True:
            try:
                self.__logger.info(f"Application <{application} is still running.")
                # Checking for peremptory finishing request
                if self.__kill_event.is_set() or self.__stop_event.is_set():
                    self.__logger.info(f"going to signal PID={popen_process_pid} "
                                    f"to terminate")
                    popen_process.terminate()
                    os.sched_yield()
                    time.sleep(0.001)  # let the Popen process to finish
                    try:
                        popen_process.wait(timeout=1)
                    except subprocess.TimeoutExpired:
                        self.__logger.info(f"will signal PID={popen_process_pid} "
                                        f"to be forced to quit")
                        # quit the process forcefully
                        popen_process.kill()

                    # whether the process finishes
                    stopping_rc = popen_process.poll()
                    if stopping_rc is None:
                        self.__logger.info(f"could not conclude the process "
                                           f"PID={popen_process.pid}")
                    else:
                        self.__logger.info(f"concluded PID={popen_process_pid} "
                                           f"which returns rc={stopping_rc}")
                    break
                # Process the Popen stdout and stderr outcomes
                try:
                    poll_rc = popen_process.poll()
                    stdout_line = popen_process.stdout.readline()
                    stderr_line = popen_process.stderr.readline()
                except KeyboardInterrupt:
                    self.__logger.info(f"KeyboardInterrupt caught by application "
                                       f"<{application}>")
                    continue
                if poll_rc is not None and not stdout_line and not stderr_line:
                    # spawned action has finished and there is no more bytes
                    # on the stdout stderr to be gathered
                    # leave the get output loop
                    break

                if stdout_line:
                    # get line from application output stream
                    self.__logger.info(f"{application}: "
                                       f"{stdout_line.strip()}")
                if stderr_line:
                    # error reported by the application
                    self.__logger.error(f"{application}: "
                                        f"{stderr_line.strip()}")
                # relinquishing the CPU window time for a while
                os.sched_yield()
            except KeyboardInterrupt:
                self.__logger.info(f"KeyboardInterrupt caught by application "
                                   f"<{application}>")
                continue

        # The application finishes
        return_code = popen_process.poll()
        if not return_code == 0:
            self.__logger.error(f"application {application} execution went "
                                f"wrong, finished returning rc={return_code}")
            self.__application_manager_response_queue.put(Response.ERROR)

        self.__logger.info(f'Application <{application}> finished properly.')
        # stop resource monitoring
        if self.__is_monitoring_enabled:
            resource_usage_monitor.keep_monitoring = False
            resources_usage_by_popen_process = resource_usage_monitor.\
                get_resource_usage_stats(return_code)
            self.__logger.debug(f"Resource Usage stats: "
                                f"{resources_usage_by_popen_process.items()}")
            # write metrics to Database (i.e. a file at the moment)
            metrics_output_directory = \
                self._configurations_manager.make_directory(
                    'Resource usage metrics', directory_path='AC results')
            metrics_file = os.path.join(metrics_output_directory,
                                        f'pid_{popen_process_pid}'
                                        '_resource_usage_metrics.json')
            self.__db_manager_file.write(metrics_file, resource_usage_monitor.
                                         get_resource_usage_stats(return_code))
        self.__application_manager_response_queue.put(Response.OK)
