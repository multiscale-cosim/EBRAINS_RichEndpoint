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
import collections


class ResourceUsageSummary:
    """
    Provides the summary of the resource usage statistics
    """
    def __init__(self) -> None:
        # NOTE: later it will be extended to also suppport usage stats of
        # other resources
        self.__cpu_usage_stats = []
        self.__memory_usage_stats = []

    @property
    def cpu_usage_stats(self): return self.__cpu_usage_stats

    # @property
    def mean_cpu_usage(self, per_cpu_avg_usage): 
        """returns the mean value of the total cpu usage"""
        # comput the mean from list of tuples: [(timnestamp, cpu_usage_stats)]
        return [sum(sub_list) / len(sub_list)
                for sub_list in [list(zip(*per_cpu_avg_usage))[1]]]

    @property
    def mean_memory_usage(self):
        """returns the mean value of the total memory usage"""
        # extract the memory usage stats from list of tuples: [(timnestamp, memory_usage_stats)]
        memory_usage = list(map(lambda x: x[1], self.__memory_usage_stats))
        counter = collections.Counter() 
        frequency = 0
        for usage_stat in memory_usage:
            counter.update(usage_stat)
            frequency += 1
        result = dict (counter)
        avg = lambda sum_value, frequency: sum_value/frequency
        mean = [{key: avg(sum_value, frequency) for key,sum_value in result.items()}]
        return mean

    @cpu_usage_stats.setter
    def cpu_usage_stats(self, cpu_usage):
        self.__cpu_usage_stats.append(cpu_usage)

    @property
    def memory_usage_stats(self): return self.__memory_usage_stats

    @memory_usage_stats.setter
    def memory_usage_stats(self, memory_usage):
        self.__memory_usage_stats.append(memory_usage)
