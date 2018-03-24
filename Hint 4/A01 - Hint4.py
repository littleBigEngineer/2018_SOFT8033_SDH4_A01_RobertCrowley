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
# FUNCTION my_main
# ------------------------------------------

def get_project(x, per_language_or_project):
  
  if "." in x.split()[0]:
    if per_language_or_project:
      key = x.split()[0].split(".")[0]
    else:
      key = x.split()[0].split(".")[1]
  else:
    if per_language_or_project:
      key = x.split()[0].split(".")[0]
    else:
      key = "wikipedia"
  try:
    val = (key, int(x.split()[2]))
  except:
    val = (key, int(x.split()[-2]))      
  return val

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(dataset_dir, o_file_dir, per_language_or_project):
    dbutils.fs.rm(o_file_dir, True)
    inputRDD = sc.textFile(dataset_dir)
    inputRDD.persist()
    total_val = inputRDD.map(lambda x: int(x.split()[-2])).sum()
    mapRDD = inputRDD.map(lambda x: get_project(x, per_language_or_project))
    combinedRDD = mapRDD.combineByKey(lambda value: (value, 1),lambda x, value: (x[0] + value, x[1] + 1),lambda x, y: (x[0] + y[0], x[1] + y[1]))
    outputRDD = combinedRDD.map(lambda x: (x[0], (x[1][0], str(format(float(x[1][0]) * 100/total_val, '.10f')+"%"))))
    outputRDD.saveAsTextFile(o_file_dir)
      
# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    dataset_dir = "/FileStore/tables/A01_my_dataset/"
    o_file_dir = "/FileStore/tables/A01_my_result/"

    per_language_or_project = False  # True for language and False for project

    my_main(dataset_dir, o_file_dir, per_language_or_project)
