#include <math.h>

#include <iostream>
#include "Table.h"

double mysqrt(double inputValue)
{
	//std::cout << "Hello World" << std::endl;
	if (inputValue == 0)
	{
		return sqrtTable[0];
	}
	// if we have both log and exp then use them
#if defined (HAVE_LOG) && defined (HAVE_EXP)
    std::cout << "Have Log" << std::endl;
    return exp(log(inputValue)*0.5);
#else // otherwise use an iterative approach
	std::cout << "Normal version" << std::endl;
	return sqrt(inputValue);
#endif
};

