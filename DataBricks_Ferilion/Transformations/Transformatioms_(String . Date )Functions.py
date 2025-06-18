# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

String_df1 = spark.read.option("Header",True).option("inferschema",True).json("/FileStore/tables/order_data.json")

# COMMAND ----------

# display(dbutils.fs.ls("/FileStore/tables/"))
display(String_df1)

# COMMAND ----------

# MAGIC %md
# MAGIC #String [Functions](url)

# COMMAND ----------

# Upper_string = String_df1.select(upper(col("product")))
# init_string = String_df1.select(initcap(col("procduct")))
lower_string = String_df1.select(lower(col("products")))

# COMMAND ----------

display(init_string)

# COMMAND ----------

# MAGIC %md
# MAGIC #Slpit

# COMMAND ----------

slipt_string = String_df1.select(split(col("customer_name"),' '))
display(slipt_string)

# COMMAND ----------

# MAGIC %md
# MAGIC #indexing
# MAGIC

# COMMAND ----------

slipt_string1 = String_df1.withColumn("customer_name",split(col("customer_name"), ' ')[0])
display(slipt_string1)

# COMMAND ----------

# MAGIC %md
# MAGIC #Date Functions
# MAGIC

# COMMAND ----------

date_df1 = String_df1.withColumn("Newdate",current_date())
display(date_df1)

# COMMAND ----------

# date_df2 = String_df1.withColumn("Newdate",date_add(current_date(),30))
date_df2 = String_df1.withColumn("Newdate",date_add(col("order_date"),3))

display(date_df2)

# COMMAND ----------

date_df3 = String_df1.withColumn("Newdate",date_sub(col("order_date"),3))

display(date_df3)

# COMMAND ----------

date_df4 = String_df1.withColumn("Date Diff",datediff(current_date(),col("order_date")))

display(date_df4)


# COMMAND ----------

date_df4 = String_df1.withColumn("end of month",month(current_date()))
display(date_df4)

# COMMAND ----------

