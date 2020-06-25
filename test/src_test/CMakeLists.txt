set(executable "${PROJECT_BINARY_DIR}/src/Tutorial")
include(CTest)
# does the application run
add_test (TutorialRuns ${executable} 25)

# does it sqrt of 25
add_test (TutorialComp25 ${executable} 25)
 
set_tests_properties (TutorialComp25 
  PROPERTIES PASS_REGULAR_EXPRESSION "25 is 5")

  

# does it sqrt of 0 special case in generated file
add_test (TutorialComp0 ${executable} 0)
 
set_tests_properties (TutorialComp0
  PROPERTIES PASS_REGULAR_EXPRESSION "0 is 100")
  
  
  
# does it handle negative numbers
add_test (TutorialNegative ${executable} -25)
set_tests_properties (TutorialNegative
  PROPERTIES PASS_REGULAR_EXPRESSION "-25 is -nan")
 
# does it handle small numbers
add_test (TutorialSmall ${executable} 0.0001)
set_tests_properties (TutorialSmall
  PROPERTIES PASS_REGULAR_EXPRESSION "0.0001 is 0.01")
 
# does the usage message work?
add_test (TutorialUsage ${executable})
set_tests_properties (TutorialUsage
  PROPERTIES 
  PASS_REGULAR_EXPRESSION "Usage:.*number")
  
##############################################  
#define a macro to simplify adding tests, then use it
macro (do_test arg result)
  add_test (TutorialComp${arg} ${executable} ${arg})
  set_tests_properties (TutorialComp${arg}
    PROPERTIES PASS_REGULAR_EXPRESSION ${result})
endmacro (do_test)
 
# do a bunch of result based tests
do_test (36 "36 is 6")
do_test (-25 "-25 is -nan")  