# -----------------------------------------------------------------------------
#  Copyright 2020 Forschungszentrum Jülich GmbH
# "Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements; and to You under the Apache License,
# Version 2.0. "
#
# Forschungszentrum Jülich
#  Institute: Institute for Advanced Simulation (IAS)
#    Section: Jülich Supercomputing Centre (JSC)
#   Division: High Performance Computing in Neuroscience
# Laboratory: Simulation Laboratory Neuroscience
#       Team: Multi-scale Simulation and Design
# -----------------------------------------------------------------------------
import pickle
import base64

def parse_command(logger, command):
        """
        helper function to parse commands received by Application Companion,
        Command & Control Service and Application Manager.
        It returns the ControlCommand object, steering command and the
        parameters.
        """
        # deserialize ControlCommand object
        control_command = pickle.loads(base64.b64decode(command))
        # parse to get steering command and the parameters
        current_steering_command, parameters = control_command.parse()
        logger.debug(f"steering command: {current_steering_command.name}, "
                     f"parameters: {parameters}")
        return control_command, current_steering_command, parameters