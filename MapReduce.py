from __future__ import print_function
import sys
from operator import add
import os
from pyspark.sql import SparkSession

#os.environ["PYSPARK_PYTHON"]="/usr/bin/python3"


if __name__ == "__main__":
    

    spark = SparkSession\
        .builder\
        .appName("PythonWordCount")\
        .getOrCreate()

     #Disable the Logging***************
    spark.sparkContext.setLogLevel("OFF")   

    lines = spark.read.text('Tags.csv').rdd.map(lambda r: r[0])


    counts = lines.flatMap(lambda x: x.split(' ')) \
                  .map(lambda x: (x, 1)) \
                  .reduceByKey(add)


    output = counts.collect()

   


    for (word, count) in output:
        print("%s: %i" % (word, count))

    spark.stop()