# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

 customer_df = spark.read.option("Header",True).option("inferschema",True).csv("/FileStore/tables/classic_data/customers.csv")
 payments_df = spark.read.option("Header",True).option("inferschema",True).csv("/FileStore/tables/classic_data/payments.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC #Rownumber

# COMMAND ----------

rownumber_df = payments_df.withColumn("RowNUmber",row_number().over(Window.orderBy(col("amount").desc())))
display(rownumber_df)


# COMMAND ----------

# MAGIC %md
# MAGIC #cummstative sum === agg-sum with window function eg running sal

# COMMAND ----------

