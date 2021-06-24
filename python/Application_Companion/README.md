# Application Companion

A lightweight application for managing and monitoring of integrated applications.

## Basic Description
It is a lightweight application offering following two main functionality:

1. **Management**: It manages the execution flow of the integrated applicatins through steering commands
such as Init, start, Pause, Resume, End.

1. **Monitoring**: It provides the following integrated applicaiton specific information:
   * **Resource utilization**: Resource usage information sucha as 
      * average CPU consumption per second in percentage, 
      * memory (VSS, RSS, PSS, USS) consumption per second
   * **Application metadata**: For example, PID, Exit code, Process name, Affinity mask, etc.
   * **Performance** Execution Time
   * **Underlying Platform**: It includes the following:
      * Hardware: node network name, number of cores, architecture, etc.
      * OS: name, release, version, etc.
      * Python: version, build, compiler etc.


## Dependency

It depends upon the  **<a href="/python/configuration_manager"> Configurations Manager </a>** for manipulation of output directories and having a uniform settings for the logs.

## Example

The following example uses a naive implementation of the multiplication of two square dense metrices to stress the CPU for testing the funcitonality of **Applicaiton Companion**. The example applicaiton named *naivemxm.py* is provided under sub-directory **example**.

```bash
# set enviornment variable
$ export PYTHONPATH=/path/to/common-utils

# run
$  /usr/bin/python3 /path/to/Application_Companion/ms_manager.py --app /path/to/Application_Companion/example/naive_mxm.py --param 300
```

A JSON file  named pid_[PID]_resource_usage_metrics.json containing the resource usage stats of the application is created after successful completion of the program under *$HOME/AC results* directory.

## Dependency

It depends upon the  **Configurations Manager** for directories manipulation and uniform settings of the logs.


## License

Copyright 2020 Forschungszentrum Jülich GmbH  
"Licensed to the Apache Software Foundation (ASF) under one or more contributor
license agreements; and to You under the Apache License, Version 2.0. "