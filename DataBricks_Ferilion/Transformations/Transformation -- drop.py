# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# dbutils.fs.ls("/FileStore/tables")

# COMMAND ----------

# path2 = "dbfs:/FileStore/tables/drivers.json"
path2 = "dbfs:/FileStore/tables/orders.csv"

try:
    dbutils.fs.ls(path2)
    # df1 = spark.read.option("Header",True).json(path2)
    Orders_df1 = spark.read.option("Header",True).csv(path2)
    print(True)
except :
    print(False)

# COMMAND ----------

# MAGIC %md
# MAGIC #With Column
# MAGIC

# COMMAND ----------

Orders_df2 = Orders_df1.withColumn("Change_Data",lit("NUll"))

# COMMAND ----------

Orders_df3  = Orders_df2.withColumn("Change_data",regexp_replace(col("Change_data"),"NUll","Yes"))

# COMMAND ----------

Orders_df4 =   Orders_df3.withColumnRenamed("Change_data","isActive")

# COMMAND ----------

# Orders_df5 = Orders_df4.withColumn("isActive",regexp_replace(col("isActive"),"Yes","No")).filter((col("status") == "cancelled"))

Orders_df5 = Orders_df4.withColumn("isActive",when(col("status") == "cancelled",lit("No")).otherwise(col("isActive")))

# COMMAND ----------

display(Orders_df5)

# COMMAND ----------

# MAGIC %md
# MAGIC #Drop
# MAGIC

# COMMAND ----------

drop_df1 = Orders_df4
drop_df1.display()

# COMMAND ----------

drop_df2 = drop_df1.drop(col("isActive")
# drop_df2 = drop_df1.drop(col("isActive"),col("status"))

# COMMAND ----------

drop_df2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Drop Duplicates
# MAGIC

# COMMAND ----------

drop_dup_df1 = Orders_df4
drop_dup_df1.display()

# COMMAND ----------

# drop_dup_df2 = drop_dup_df1.dropDuplicates() ---Drop duplicates if entire row is dup
drop_dup_df2 = drop_dup_df1.drop_duplicates(["customer_id"]) # check in dup single column and delete
drop_dup_df2 = drop_dup_df1.drop_duplicates(["customer_id","product_id"]) # check in dup cob of both column and delete

# COMMAND ----------

drop_dup_df2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC drop NUll vallues

# COMMAND ----------

drop_dup_df3 = drop_dup_df2.dropna() # drop  if entre row is null

# b) Drop rows where specific column(s) is null:

df_clean = drop_dup_df2.dropna(["order_id", "item_weight"]) 