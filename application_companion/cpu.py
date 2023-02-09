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
from collections import OrderedDict


class CPU:
    '''
    Provides the detailed information about the type of individual CPUs
    (processors) for all the CPUs present on the system.
    '''
    def __init__(self):
        self.__cpu_info = OrderedDict()
        self.__proc_info = OrderedDict()
        self.__nprocs = 0

    def __copy_data_and_reset(self):
        # copy data
        self.__cpu_info[f'processor-{self.__nprocs}'] = self.__proc_info
        # increment the number of parsed processors
        self.__nprocs = self.__nprocs+1
        # reset to store next processor's information
        self.__proc_info = OrderedDict()

    def __parse_line(self, line):
        if len(line.split(':')) == 2:
            # Case a, if information is availabl e.g. 'cpu family : 6'
            # store it as key, value pair separated by ':'
            self.__proc_info[line.split(':')[0].strip()] = \
                line.split(':')[1].strip()
        else:
            # Case b, if information is missing e.g. 'power management: '
            # split it as key, value pair and store the value as an empty
            # string i.e. ''
            self.__proc_info[line.split(':')[0].strip()] = ''

    def detailed_info(self):
        '''
        Returns the information in /proc/cpuinfo as a dictionary in the
        following format:
        cpu_info['processor-0']={...}
        cpu_info['processor-1']={...}
        '''
        with open('/proc/cpuinfo') as f:
            # read line
            for line in f:
                #
                if not line.strip():
                    # end of data belonging to a processor
                    # copy data and reset proc_info variable
                    self.__copy_data_and_reset()
                else:
                    # keep parsing data belongs to the same processor
                    self.__parse_line(line)

        # return CPUs detailed information
        return self.__cpu_info
