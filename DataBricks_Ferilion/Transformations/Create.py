# Databricks notebook source
# dbutils.fs.rm("/FileStore/tables/classic_data/",recurse=True )
dbutils.fs.mkdirs("/FileStore/tables/emp_data")

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables")

# COMMAND ----------

display(dbutils.fs.ls("/FileStore/tables/"))

# COMMAND ----------

#copy from data from one folder to another folder
dbutils.fs.cp("dbfs:/FileStore/tables/parquet_file", 
              "dbfs:/FileStore/tables/ORC_file", 
              recurse=True)

# Step 2: Delete the old folder
dbutils.fs.rm("dbfs:/FileStore/tables/parquet_file", recurse=True)

# COMMAND ----------

