###############################################
# Add C++ tests

###############################################
# Call the python_unittest_runner on the python test dir
add_test(python_tests bash env_test_runner.sh "./python")

################################################
# Add cython tests, they need environment,
# This is set in the the cython_env test runner
configure_file(env_test_runner.sh.in env_test_runner.sh)
add_test(cython_tests bash env_test_runner.sh "./cython")
