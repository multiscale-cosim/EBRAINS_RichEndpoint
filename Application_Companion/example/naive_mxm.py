#!/usr/bin/python3
# ------------------------------------------------------------------------------
#  Copyright 2020 Forschungszentrum Jülich GmbH and Aix-Marseille Université
# "Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements; and to You under the Apache License,
# Version 2.0. "
#
# Forschungszentrum Jülich
# Institute: Institute for Advanced Simulation (IAS)
# Section: Jülich Supercomputing Centre (JSC)
# Division: High Performance Computing in Neuroscience
# Laboratory: Simulation Laboratory Neuroscience
# Team: Multi-scale Simulation and Design
# ------------------------------------------------------------------------------
"""
    A naive implementation of two dense square matrix-matrix multipliction
    to stress the CPU for testing the resource usage monitoring purposes.
"""

import random
import sys


random.seed(1234)


def create_random_matrix(N, maxVal=1000):
    matrix = []
    for i in range(N):
        matrix.append([random.randint(0, maxVal) for el in range(N)])
    return matrix


def _mxm(A, B, N):
    # initialize result matrix with 0
    C = create_random_matrix(N, 0)
    # dot product A x B
    for i in range(len(A)):
        for j in range(len(B[0])):
            for k in range(len(B)):
                C[i][j] += A[i][k] * B[k][j]
    # C = [[sum(a*b for a,b in zip(A_row,B_col)) for B_col in zip(*B)] for A_row in A]
    return C


def naive_mxm(N=300):
    matrixA = create_random_matrix(N)
    # print(f'matrixA = {matrixA}')
    matrixB = create_random_matrix(N)
    # print(f'matrixB = {matrixB}')
    print(f'staring {N} x {N} matrix multiplication')
    matrixC = _mxm(matrixA, matrixB, N)
    # print(f'matrixC = {matrixC}')
    print('done mxm!')


if __name__ == '__main__':
    number_of_iterations = (int)(sys.argv[1])
    naive_mxm(number_of_iterations)
