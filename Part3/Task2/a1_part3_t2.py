#!/usr/bin/env python
# coding: utf-8

# In[2]:


import re
import sys
from operator import add
from typing import Iterable, Tuple

from pyspark.resultiterable import ResultIterable
from pyspark.sql import SparkSession
import pyspark
from pyspark.sql import functions as sf


# In[3]:


# Helper function to calculates URL contributions to the rank of other URLs
def computeContribs(urls: ResultIterable[str], rank: float) -> Iterable[Tuple[str, float]]:
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)


# In[4]:


# Helper function to parses a urls string into urls pair
def parseNeighborURLs(urls: str) -> Tuple[str, str]:
    parts = re.split(r'\s+', urls)
    return parts[0], parts[1]


# In[16]:


if __name__ == "__main__":
	# A baseline implementation should take three command-line arguments except the python code
	# You can extend this by including however many arguments you want
#     if len(sys.argv) != 4:
#         print("Usage: pagerank <path_to_input> <path_to_output> <iterations>", file=sys.stderr)
#         sys.exit(-1)

    # Initialize the Spark context
    spark = SparkSession\
        .builder\
        .appName("AppPageRank")\
        .master("spark://172.31.41.240:7077")\
        .getOrCreate()

    # Specifying the number of ways to split dataset
    partitions = int(sys.argv[4])
    
    # Loads in input file
    #     URL         neighbor URL
    #     URL         neighbor URL
    #     URL         neighbor URL
    #     ...
    lines = spark.sparkContext.textFile("hdfs://172.31.41.240:9000/"+sys.argv[1])

    # Perform a transformation to define a links RDD by using parseNeighborURLs helper function
    links = lines.map(lambda urls: parseNeighborURLs(urls)).distinct().groupByKey()

    # Initialize a ranks RDD
    ranks = links.map(lambda url_neighbors: (url_neighbors[0], 1.0))

    # Calculates and updates URL ranks continuously using PageRank algorithm.
    for iteration in range(int(sys.argv[3])):
        # TODO: Implement the PageRank algorithm here
        # TODO: Implement the PageRank algorithm here
        contribs = links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(
            url_urls_rank[1][0], url_urls_rank[1][1] #type: ignore[arg-type]
        ))
        ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15).partitionBy(partitions)


    # Dump output to HDFS
    ranksDf = ranks.toDF()
    ranksDf.write.format("csv").mode("overwrite").save("hdfs://172.31.41.240:9000/" +sys.argv[2])

	# Stopping Spark Session
    spark.stop()

