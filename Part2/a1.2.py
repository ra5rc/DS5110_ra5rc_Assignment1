#!/usr/bin/env python
# coding: utf-8

# In[3]:

#Reading in spark functionality to work with RDD, dataframes, and datasets
from pyspark.sql import SparkSession


# In[4]:

#Creating SparkSession instance
spark = SparkSession.builder.appName("a1").master("spark://172.31.41.240:7077").getOrCreate()


# In[5]:

#reading in data from file into dataframe
df = spark.read.load("hdfs://172.31.41.240:9000/export.csv",format="csv",inferSchema="true",sep=",",header="true")


# In[8]:

#sorting the country codes alphabetically and timsetamp descending
df=df.sort("cca2","timestamp")


# In[10]:

#when data exists we specify to overwrite old version with new version and save it as a csv with name a1_part2
df.write.mode("overwrite").csv("a1_part2")


# In[11]:

#ending spark program
spark.stop()

