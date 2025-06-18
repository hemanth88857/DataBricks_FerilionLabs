# Databricks notebook source
# using RDD
data = [1,2,3,4,5,6,7,7,8,8,9,56,56,57,53,3,53,5,6,57,34,4,5,5,6,6,7,4,4,3,3]
rdd = spark.sparkContext.parallelize(data,3)
data1 = rdd.map(lambda x:x*2)
result = data1.collect()
print(result)

# COMMAND ----------

# DBTITLE 1,Task 1: Word Count from List


data = ["hello world", "hello databricks", "hello spark"]

rdd = spark.sparkContext.parallelize(data)
split_each_word = rdd.flatMap(lambda x:x.split(" "))
each_word = split_each_word.map(lambda x:(x,1))
count_of_each_word = each_word.reduceByKey(lambda a,b : a+b)

result  = count_of_each_word.collect()
print(result)

# COMMAND ----------


import os

path = r"\Users\Dell\Desktop\Azure data\DataBricks\orders.csv"
if os.path.exists(path) : 
    print(True)
else :
    print(False)


# df = spark.read.csv("C:\Users\Dell\Desktop\Azure data\DataBricks")

# COMMAND ----------

# MAGIC %sql
# MAGIC

# COMMAND ----------

