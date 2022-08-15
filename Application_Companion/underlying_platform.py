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
from __future__ import print_function
import platform
import os

from EBRAINS_RichEndpoint.Application_Companion.cpu import CPU


class Platform:
    """
    Provides the underlying Platform details.
    """
    def __init__(self) -> None:
        self.__info = {}
        # Hardware specific details
        self.__info["system network name"] = self.ComputerName
        self.__info["processor name"] = self.processor
        self.__info["number of cores"] = self.number_of_CPU_cores
        self.__info["machine type"] = self.machine
        self.__info["architectural detail"] = self.architecture

        # OS specific details
        self.__info["OS Name"] = self.OS_name
        self.__info["OS release"] = self.OS_release
        self.__info["OS Version"] = self.OS_version
        self.__info["Linux Kernel Version"] = self.platform_details

        # Python specific details
        self.__info["Python version"] = self.python_version
        self.__info["Python Build info"] = self.python_build
        self.__info["Compiler info"] = self.python_compiler
        self.__info["Implementation"] = self.python_implementation
        self.__CPU = CPU()

    @property
    def basic_info(self): return self.__info

    @property
    def detailed_CPUs_info(self): return self.__CPU.detailed_info()

    @property
    def platform_details(self): return platform.platform()

    @property
    # system network name
    def ComputerName(self): return platform.node()

    @property
    def number_of_CPU_cores(self): return os.cpu_count()

    @property
    def isLinux(self): return (self.OS_name == 'Linux')

    @property
    def isMacOSX(self): return (self.OS_name == 'Darwin')

    @property
    def isWindows(self): return (self.OS_name == 'Windows')

    @property
    def OS_name(self): return platform.system()

    @property
    # returns a namedtuple() containing six attributes:
    # system, node, release, version, machine, and processor
    def uname(self): return platform.uname()

    @property
    def OS_release(self): return platform.release()

    @property
    def OS_version(self): return platform.version()

    @property
    # width or size of registers available in the core
    def machine(self): return platform.machine()

    @property
    def processor(self): return platform.processor()

    @property
    # returns a tuple that stores information about the bit architecture
    def architecture(self): return platform.architecture()

    @property
    # python build date and no.
    def python_build(self): return platform.python_build()

    @property
    # python compiler
    def python_compiler(self): return platform.python_compiler()

    @property
    # python Source Code Manager (SCM)
    def python_SCM(self): return platform.python_branch()

    @property
    # python implementation (e.g. CPython, JPython, etc.)
    def python_implementation(self): return platform.python_implementation()

    @property
    # python version (returned in following manner major.minor.patchlevel)
    def python_version(self): return platform.python_version()
