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

    @cpu_usage_stats.setter
    def cpu_usage_stats(self, cpu_usage):
        self.__cpu_usage_stats.append(cpu_usage)

    @property
    def memory_usage_stats(self): return self.__memory_usage_stats

    @memory_usage_stats.setter
    def memory_usage_stats(self, memory_usage):
        self.__memory_usage_stats.append(memory_usage)
