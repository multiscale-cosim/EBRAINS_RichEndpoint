# configure a header file to pass some of the CMake settings
# to the source code
configure_file (
  "${PROJECT_SOURCE_DIR}/src/tutorialconfig.h.in"
  "${PROJECT_BINARY_DIR}/src/tutorialconfig.h"
  )

 ###################################
# Add dir with possible library
if (USE_MYMATH)
  include_directories ("${PROJECT_SOURCE_DIR}/src/lib")
  add_subdirectory (lib)
  set (EXTRA_LIBS ${EXTRA_LIBS} MathFunctions)
endif (USE_MYMATH)  

  
add_executable(Tutorial tutorial.cpp)

target_link_libraries (Tutorial ${EXTRA_LIBS} )

# add the install targets
install (TARGETS Tutorial DESTINATION bin)
install (FILES "${PROJECT_BINARY_DIR}/src/tutorialconfig.h"        
         DESTINATION include) 

install (TARGETS Tutorial DESTINATION bin)		 
		 

#################################
# Add external cython so
add_custom_target(
   always_run ALL
   COMMAND make
   WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/cython_lib"
)

# Install the the shared lib
install (FILES "${CMAKE_CURRENT_SOURCE_DIR}/cython_lib/_routines.so" 
		  DESTINATION lib/cython_lib)
#install the interface __init__ file
install (FILES "${CMAKE_CURRENT_SOURCE_DIR}/cython_lib/__init__.py" 
		  DESTINATION lib/cython_lib)
# This means that we need to put the lib path in the pythonpath
# TODO: Install in the python path..

		  
		  
		  


