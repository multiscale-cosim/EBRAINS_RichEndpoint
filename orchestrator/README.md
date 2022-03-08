# Orchestrator

It manages the workflow execution by orchestrating and steering the other components (Application Companions). It keeps the workflow synchronized and the global state and status as validated.

## Basic Description
It is the key component that offers the following functionality:

1. **Steering and Management**: It steers and manages the execution flow of the workflow. It stops execution if the global state is ERROR due to inconsistent local states.
1. **Synchronization**: It synchronizes the overall workflow by determining the minimum step-size for execution.
1. **Global Status Management**: It manages the over all system status by keeping track of local statuses of the components.
1. **Global State transition Management**: It manages the global state transition of the workflow during its life cycle by validating the local states.
1. **Monitoring**: It monitors the local states and statuses of all components to validate the global state.


## Dependency
It depends upon the following modules:
1.  **<a href="/python/configuration_manager"> Configurations Manager </a>** for manipulation of output directories and having a uniform settings for the logs.
1. **<a href="/python/Application_Companion"> Application_Companion </a>** for orchestration of the workflow.

## Example

The following example uses a naive implementation of the multiplication of two square dense metrices to stress the CPU for testing the functionality of **Application Companion**. The example application named *naivemxm.py* is provided under sub-directory **example**.

```bash
# set HOME variable
$ export HOME=/path/to/EBRAINS-RichEndpoint

# set environment variable
$ export PYTHONPATH=/path/to/EBRAINS-RichEndpoint

# run
$  /usr/bin/python3 /path/to/Application_Companion/ms_manager.py --app /path/to/Application_Companion/example/naive_mxm.py --param 300
```

The logs and JSON file(s) named pid_[PID]_resource_usage_metrics.json containing the resource usage stats of the integrated application(s) are created after successful completion of the program under *$HOME/EBRAINS-RichEndpoint_outputs* directory.


## License

Copyright 2020 Forschungszentrum Jülich GmbH  
"Licensed to the Apache Software Foundation (ASF) under one or more contributor
license agreements; and to You under the Apache License, Version 2.0. "