# -*- coding: utf-8 -*-
#
# cython: cdivision=True
# cython: wraparound=False
#

import cython

# Floating point types to use with NumPy
#
# IEEE 754 says that float is 32 bits and double is 64 bits
#
ctypedef double dtype_float_t

cdef extern from "_routines.hpp":

    dtype_float_t squared_simple_cython(const dtype_float_t)


@cython.boundscheck(False)
def squared_simple(dtype_float_t input):
    """
    return square of input

    :param input a double
    :return: square of input
    """
    return squared_simple_cython(input)

