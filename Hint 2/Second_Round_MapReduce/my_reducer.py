#!/usr/bin/python

# --------------------------------------------------------
#           PYTHON PROGRAM
# Here is where we are going to define our set of...
# - Imports
# - Global Variables
# - Functions
# ...to achieve the functionality required.
# When executing > python 'this_file'.py in a terminal,
# the Python interpreter will load our program,
# but it will execute nothing yet.
# --------------------------------------------------------

import sys
import codecs
from operator import itemgetter

# ------------------------------------------
# FUNCTION my_reduce
# ------------------------------------------
def my_reduce(input_stream, total_petitions, output_stream):
    perc_dict = {}

    for input in input_stream:
        if input.split()[0] not in perc_dict:
            perc_dict[input.split()[0]] = int(input.split()[1][1:-1])
        else:
            perc_dict[input.split()[0]] += int(input.split()[1][1:-1])


    for key, value in perc_dict.items():
        perc_dict[key] = (value, str(format(value/int(total_petitions)*100, '.10f')) + "%")
        
    for key, value in sorted(perc_dict.items(), key=itemgetter(1), reverse=True):
        output_stream.write(key + "\t(" + str(value[0]) + ", " + str(value[1]) + ")\n")

    pass

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(debug, i_file_name, total_petitions, o_file_name):
    # We pick the working mode:

    # Mode 1: Debug --> We pick a file to read test the program on it
    if debug == True:
        my_input_stream = codecs.open(i_file_name, "r", encoding='utf-8')
        my_output_stream = codecs.open(o_file_name, "w", encoding='utf-8')
    # Mode 2: Actual MapReduce --> We pick std.stdin and std.stdout
    else:
        my_input_stream = sys.stdin
        my_output_stream = sys.stdout

    # We launch the Map program
    my_reduce(my_input_stream, total_petitions, my_output_stream)

# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    # 1. Input parameters
    debug = True

    # This variable must be computed in the first stage
    file = open("../First_Round_MapReduce/reduce_simulation.txt")
    for val in file:
        total_petitions = val.split(',')[1][:-1]

    # total_views = "../../../Second_Round_MapReduce/"

    i_file_name = "sort_simulation.txt"
    o_file_name = "reduce_simulation.txt"

    my_main(debug, i_file_name, total_petitions, o_file_name)
