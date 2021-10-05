<div align="center" id="top"> 
  <img src="../../../misc/logo.jpg" alt="EBRAINS-RichEndpoint" />

  &#xa0;

  <!-- <a href="git@github.com:multiscale-cosim/EBRAINS-RichEndpoint.git">Demo</a> -->
</div>

<h1 align="center">EBRAINS-RichEndpoint</h1>

<p align="center">
  <img alt="Github top language" src="https://img.shields.io/github/languages/top/multiscale-cosim/EBRAINS-RichEndpoint?color=56BEB8" />

  <img alt="Github language count" src="https://img.shields.io/github/languages/count/multiscale-cosim/EBRAINS-RichEndpoint?color=56BEB8" />

  <img alt="Repository size" src="https://img.shields.io/github/repo-size/multiscale-cosim/EBRAINS-RichEndpoint?color=56BEB8" />

  <img alt="License" src="https://img.shields.io/github/license/multiscale-cosim/EBRAINS-RichEndpoint?color=56BEB8" />

  <img alt="Github issues" src="https://img.shields.io/github/issues/multiscale-cosim/EBRAINS-RichEndpoint?color=56BEB8" />

  <img alt="Github forks" src="https://img.shields.io/github/forks/multiscale-cosim/EBRAINS-RichEndpoint?color=56BEB8" />

  <img alt="Github stars" src="https://img.shields.io/github/stars/multiscale-cosim/EBRAINS-RichEndpoint?color=56BEB8" />
</p>

## Status

<h4 align="center"> 
	🚧  EBRAINS-RichEndpoint 🚀 Under construction...  🚧
</h4> 

<hr>

<p align="center">
  <a href="#dart-about">About</a> &#xa0; | &#xa0; 
  <a href="#sparkles-features">Features</a> &#xa0; | &#xa0;
  <a href="#rocket-technologies">Technologies</a> &#xa0; | &#xa0;
  <a href="#white_check_mark-requirements">Requirements</a> &#xa0; | &#xa0;
  <a href="#checkered_flag-starting">Starting</a> &#xa0; | &#xa0;
  <a href="#memo-license">License</a> &#xa0; | &#xa0;
  <a href="https://github.com/multiscale-cosim" target="_blank">Author</a>
</p>

<br>

## :dart: About ##

The RichEndpoint is a set of functionality implemented in the following modules:

* <a href="/python/Application_Companion"> Application Companion </a>
* <a href="/python/Orchestrator"> Orchestrator </a>
* <a href="/python/Orchestrator/command_steering_service.py"> Command and Steering service </a>
* Communication Protocol (InterscaleHUB)

## :sparkles: Features ##

:heavy_check_mark: A Module shared across all the RichEndpoint modules for general functionality such as a centralized management of configuration settings, setting up directories and setting a uniform format for the logging. More details are provided <a href="/python/configuration_manager"> here</a>.

:heavy_check_mark: A lightweight application for managing and monitoring of integrated applications. More details are provided <a href="/python/Application_Companion"> here</a>.

:heavy_check_mark: A module to manage the workflow execution by orchestrating and steering the other components (Application Companions). It keeps the workflow synchronized and the global state and status as validated. More details are provided <a href="/python/Orchestrator"> here</a>.

:heavy_check_mark: A module to register and discover the all distributed component services (such as Orchestrator and Application Companions) of the workflow.

## :rocket: Technologies ##

The following tools were used in this project:

- [Python](https://www.python.org/)
- [CMake](https://cmake.org/)
- [C++](https://isocpp.org/)
- [Makefile](https://www.gnu.org/software/make/manual/make.html)

## :white_check_mark: Requirements ##

Before starting :checkered_flag:, you need to have [Python](https://www.python.org/) and [CMake](https://cmake.org/) installed.

## :checkered_flag: Starting ##

```bash
# Clone this project
$ git clone git@github.com:multiscale-cosim/EBRAINS-RichEndpoint.git

# Access
$ cd EBRAINS-RichEndpoint

# set HOME variable
$ export HOME=/path/to/EBRAINS-RichEndpoint

# set environment variable
$ export PYTHONPATH=/path/to/EBRAINS-RichEndpoint

# Install for CMake older than 3.15
$ cmake --build . --target install


# Install for CMake 3.15 and newer
$ cmake --install <dir> [<options>]

# Run the project
To be done

```

## TODO

Out of source build is currently broken
 - test runner files are missing
 - The including / building of the Table.h file goes wrong

## :memo: License ##

This project is under license from Apache License, Version 2.0. For more details, see the [LICENSE](LICENSE) file.


Made by <a href="https://github.com/multiscale-cosim" target="_blank">Multiscale Co-simulation team.</a>

&#xa0;

<a href="#top">Back to top</a>