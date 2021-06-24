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

    def detailed_info(self):
        ''' Return the information in /proc/cpuinfo as a dictionary in the
        following format:
        cpu_info['processor-0']={...}
        cpu_info['processor-1']={...}
        '''
        if not self.__cpu_info:
            with open('/proc/cpuinfo') as f:
                for line in f:
                    if not line.strip():
                        # end of one processor
                        self.__cpu_info[f'processor-{self.__nprocs}'] = self.__proc_info
                        self.__nprocs = self.__nprocs+1
                        # Reset
                        self.__proc_info = OrderedDict()
                    else:
                        if len(line.split(':')) == 2:
                            self.__proc_info[line.split(':')[0].strip()] = \
                                line.split(':')[1].strip()
                        else:
                            self.__proc_info[line.split(':')[0].strip()] = ''
        return self.__cpu_info
