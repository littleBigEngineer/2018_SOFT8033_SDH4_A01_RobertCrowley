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

# ------------------------------------------
# FUNCTION my_map
# ------------------------------------------
def my_map(input_stream, languages, num_top_entries, output_stream):
    valid_list = []
    proj_dict = {}

    for word in input_stream:
        if word[0:2] in languages:
            if word[2:3] == '.' or word[2:3] == ' ':
                if word.split()[0] not in proj_dict:
                    proj_dict[word.split()[0]] = []
                word = word.replace(',','')  # Removing any commas
                valid_list.append(word)
                proj_dict[word.split()[0]].append(word.split()[1] + "," + word.split()[2])

    for key, val in proj_dict.items():
        new_vals = []
        for word in val:
            num = word.split(',')
            if not new_vals:
                new_vals.append(word)
            else:
                if int(new_vals[0].split(',')[1]) > int(num[1]):
                    new_vals.append(word)
                if int(new_vals[0].split(',')[1]) <= int(num[1]):
                    new_vals = [word] + new_vals
        proj_dict[key] = new_vals[:num_top_entries]

    for key, val in sorted(proj_dict.items()):
        for item in val:
            output_stream.write(key + "\t("+item+")\n")
    pass

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(debug, i_file_name, o_file_name, languages, num_top_entries):
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
    my_map(my_input_stream, languages, num_top_entries, my_output_stream)

# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    print("Starting")
    # 1. Input parameters
    debug = True

    i_file_name = "pageviews-20180219-100000_0.txt"
    o_file_name = "mapResult.txt"

    languages = ["en", "es", "fr"]
    num_top_entries = 5

    # 2. Call to the function
    my_main(debug, i_file_name, o_file_name, languages, num_top_entries)
