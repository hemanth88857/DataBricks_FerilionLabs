# Databricks notebook source
null_data = spark.read.option("Header",True).option("Inferschema",True).csv("/FileStore/tables/BigMartSales.csv")

# COMMAND ----------

display(null_data)

# COMMAND ----------

display(dbutils.fs.ls("/FileStore/tables/"))

# COMMAND ----------

# MAGIC %md
# MAGIC #Filling nulls

# COMMAND ----------

null_data1 = null_data.fillna("N/A")
# null_data1 = null_data.fillna(int("0"),subset=["Item_Weight"])
display(null_data1)

# COMMAND ----------

