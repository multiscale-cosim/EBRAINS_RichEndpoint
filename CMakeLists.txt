# Based on https://cmake.org/cmake-tutorial/

# Includes:
# - Files in src directory
# - Test in findable sourcedirectory, started from root folder
# - Code generation
# - Source for system resources

# TODO:
# - Add python support
# - Add support for makefiles


cmake_minimum_required (VERSION 2.6)
project (Tutorial)

# The version number.
set (Tutorial_VERSION_MAJOR 1)
set (Tutorial_VERSION_MINOR 0)

########################################
# Perform all work to be done for parsing the source three
# This will allow us to use all the defines/variables
# in the child CMakeLists

# Set options for cmake
# should we use our own math functions?
option (USE_MYMATH 
        "Use tutorial provided math implementation" ON) 
        
# Check for available system functionality		
include (CheckFunctionExists)
# does this system provide the log and exp functions?
check_function_exists (log HAVE_LOG)
check_function_exists (exp HAVE_EXP)		

# Find necessary packages
find_package( PythonLibs 3.6 REQUIRED )
include_directories( ${PYTHON_INCLUDE_DIRS} )

##################################	
# add the binary tree to the search path for include files
# so that we will find TutorialConfig.h
include_directories("${PROJECT_BINARY_DIR}/src")

# Enables adding of tests in subdirectories
ENABLE_TESTING()

add_subdirectory(src)
add_subdirectory(python)

add_subdirectory(script)
add_subdirectory(test)


###########################################
# build a CPack driven installer package
include (InstallRequiredSystemLibraries)
set (CPACK_PACKAGE_VERSION_MAJOR "${Tutorial_VERSION_MAJOR}")
set (CPACK_PACKAGE_VERSION_MINOR "${Tutorial_VERSION_MINOR}")
include (CPack)