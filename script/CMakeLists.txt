
configure_file(init.sh.in init.sh)

#install the interface __init__ file
install (FILES ${CMAKE_BINARY_DIR}/script/init.sh 
		 DESTINATION script)
