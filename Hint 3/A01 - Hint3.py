
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
import heapq
import codecs

# ------------------------------------------
# FUNCTION take_ordered_by_key
# ------------------------------------------

def take_ordered_by_key(self, num, sortValue = None, reverse=False):
 
        def init(a):
            return [a]
 
        def combine(agg, a):
            agg.append(a)
            return getTopN(agg)
 
        def merge(a, b):
            agg = a + b
            return getTopN(agg)
 
        def getTopN(agg):
            if reverse == True:
                return heapq.nlargest(num, agg, sortValue)
            else:
                return heapq.nsmallest(num, agg, sortValue)              
 
        return self.combineByKey(init, combine, merge)
 

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------

def my_main(dataset_dir, o_file_dir, languages, num_top_entries): 
    from pyspark.rdd import RDD
    RDD.takeOrderedByKey = take_ordered_by_key
       
    dbutils.fs.rm(o_file_dir, True) # clears the output file
    inputRDD = sc.textFile(dataset_dir) # Adds content of files into RDD
    validRDD = inputRDD.filter(lambda x: check_if_valid(x, languages)) # Filters the inputRDD of searches not in the languages
    mapRDD = validRDD.map(lambda x: (x.split()[0], (x.split()[0], x.split()[1], x.split()[2]))) # maps the values of validRDD by project
    sortedRDD = mapRDD.takeOrderedByKey(5, sortValue=lambda x: int(x[2]), reverse=True).flatMap(lambda x: x[1]) # top 5 entries of each project
    outputRDD = sortedRDD.map(lambda x: (x[0], (x[1], x[2]))).sortByKey(ascending=True) # sorts keys 
    outputRDD.saveAsTextFile(o_file_dir)

# ------------------------------------------
# FUNCTION check_if_valid
# ------------------------------------------
def check_if_valid(x, languages):
  valid = False
  for lang in languages:
    if(x.split()[0][0:len(lang)] in languages):
      valid = True
  return valid  
  
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

    languages = ["en", "es", "fr"]
    num_top_entries = 5

    my_main(dataset_dir, o_file_dir, languages, num_top_entries)
