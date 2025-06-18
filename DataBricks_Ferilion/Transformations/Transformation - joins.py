# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

 customer_df = spark.read.option("Header",True).option("inferschema",True).csv("/FileStore/tables/classic_data/customers.csv")
 payments_df = spark.read.option("Header",True).option("inferschema",True).csv("/FileStore/tables/classic_data/payments.csv")
 

# COMMAND ----------

display(customer_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #inner

# COMMAND ----------

inner_df = customer_df.join(payments_df,customer_df["customerNumber"]==payments_df["customerNumber"],"inner")
display(inner_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #left

# COMMAND ----------

left_df = customer_df.join(payments_df,customer_df["customerNumber"]==payments_df["customerNumber"],"Left")
display(left_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #right

# COMMAND ----------

right_df = customer_df.join(payments_df,customer_df["customerNumber"]==payments_df["customerNumber"],"Right")
display(right_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #leftanti === left outer join

# COMMAND ----------

# MAGIC %md 
# MAGIC #full join
# MAGIC cobination of left,right, common

# COMMAND ----------

