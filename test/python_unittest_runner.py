import os
import unittest
import getopt
import string
import inspect
import sys
import re

class Discover():
    """
    Discover class collects all unit test case in <path> and recursive directories
    Collects them in a single large suite.
    Start at supplied <path> an add all tests in files matching the supplied expression and
    all individual tests matching the expression 
    """
    #TODO: ordering of suites is not controlled atm.   
    #TODO: maybe it should be expanded with multi path input?? 
    suite = unittest.TestSuite()

    def __init__(self, path, pattern):

        #match with all (used for a filename matches with expression: all individual test must be loaded
        allMatcher = re.compile(".*")
        #matcher for the expression
        patternMatcher = re.compile(pattern)
        #matcher for hidden dirs
        hiddenMatcher = re.compile(".*/\..*")

        for root, dirs, files in os.walk(path):
            #skip hidden directories
            if hiddenMatcher.match(root):
                continue

            dirSuite = unittest.TestSuite()
            for name in files:
                fileNameParts = name.split('.')
                #assert correct file extention 
                if (len(fileNameParts) == 1) or (fileNameParts[1] != 'py'):
                    continue

                module = self.import_path(root, fileNameParts[0])  #use import including path

                #the expression mechanism
                testMatcher = None
                if patternMatcher.match(name):
                    testMatcher = allMatcher      #if current dir matches with expression include all tests
                else:
                    testMatcher = patternMatcher


                #create a test suite
                fileSuite = unittest.TestSuite()
                testnames = dir(module)



                #add all cases ending with test and match the regexp search string
                for testName in testnames:
                    if testName.endswith('Test') or testName.endswith('test'):
                        testClass = getattr(module, testName)    #load attribute
                        if inspect.isclass(testClass):           #if class 
                            if not testMatcher.match(testName):  #Continue of current testname does not match supplied expression
                                continue
                            fileSuite.addTest(unittest.makeSuite(testClass))

                #if tests found add the file suite to the directory suite
                if fileSuite.countTestCases() != 0:
                    dirSuite.addTest(fileSuite)

            #add to top level suite
            if dirSuite.countTestCases() != 0:
                self.suite.addTest(dirSuite)


    def import_path(self, path, filename):
        """ 
        Import a file with full path specification. Allows one to
        import from anywhere, something __import__ does not do. 
        """
        filename, ext = os.path.splitext(filename)
        sys.path.append(path)
        module = __import__(filename)
        reload(module) # Might be out of date
        del sys.path[-1]
        return module


class UnitTesterTest(unittest.TestCase):
    """
    Self test for the UnitTester
    """
    #TODO: Add propper test suite, creating come files and try laoding it (multiple directories and depths)
    def setUp(self):
        self.tester = "A test string"

    def test_validator(self):
        """
        Check that current testClass is loaded and performed
        """
        self.assertTrue(self.tester == "A test string")


def usage():
    """
    Display a short overview of available arguments
    """
    usage = r"""Usage: python python_unittest_runner [-p <path = '.'> -e <expression = *>  -h -x]
    Recursively look in path for unit test classes matching expression.
    Collect in a single suite and run them
    -p, --path <path> to start looking. Default is '.'
      -e, --exp  <expresion> to match with found classes to perform a subset of the tests
      or
      -m, --matchword match with found classes to perform a subset of tests (shorthand for .*arg.* expression
    -h, --help Display this usage
    -x, --xml  <filename> Export resuls to xml (results are overwritten) 
    """
    print usage

if __name__ == "__main__":

    #Default parameters settings
    path = '.'
    expression = '.*'
    xml = ""

    #parse command lines and set parameters for Discover function
    try:
        opts, args = getopt.getopt(sys.argv[1:], "p:e:hx:m:", ["path=", "exp=", "help", "xml=", "matchword="])
    except getopt.GetoptError:
        usage()
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("-h", "--help"):
            usage()
            sys.exit()
        elif opt in ("-p", "--path"):
            path = arg
        elif opt in ("-e", "--exp"):
            expression = arg
        elif opt in ("-x", "--xml"):
            xml = arg
        elif opt in ("-m", "--matchword"):
            expression = ".*{0}.*".format(arg)

    #Collect tests from files and paths    
    test = Discover(path, expression)

    #decide on unit testrunner to use, run it and save the results
    if xml:
        import xmlrunner
        result = xmlrunner.XMLTestRunner(output=xml).run(test.suite)
    else:
        result = unittest.TextTestRunner(verbosity=2).run(test.suite)

    #collect the numeric results using expressions
    FailedTestMatcher = re.compile(".*run=(\d+).*errors=(\d+).*failures=(\d+)")
    matches = FailedTestMatcher.match(str(result))
    runErrorFailures = matches.groups(0)

    #add to get the total number of not succesfull tests
    failingTests = int(runErrorFailures[1]) + int(runErrorFailures[2])

    #provide number of failing tests as exit value
    sys.exit(failingTests)
