# Databricks notebook source
dbutils.fs.ls("/FileStore/tables")

# COMMAND ----------

# MAGIC %md
# MAGIC #To delete the File

# COMMAND ----------

#dbutils.fs.ls("/FileStore/tables")
#dbutils.fs.rm("/FileStore/tables", recurse=True)


# COMMAND ----------

# MAGIC %md
# MAGIC reading CSV file

# COMMAND ----------

# reading file
df = spark.read.format("csv").option("header",True).load("/FileStore/tables/orders.csv")
df.display()



# COMMAND ----------

# MAGIC %md
# MAGIC Reading Json File

# COMMAND ----------

df1 = spark.read.format("json").option("Header",True).load("/FileStore/tables/order_data.json")
df1.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Reading Parquet File

# COMMAND ----------

df3 = spark.read.format("parquet").option("Header",True).load("/FileStore/tables/titanic.parquet")
df3.display()

# COMMAND ----------

# MAGIC %md
# MAGIC creating a floders in path
# MAGIC

# COMMAND ----------

dbutils.fs.mkdirs("/FileStore/tables/parquet_file")

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables")

# COMMAND ----------

df4  = spark.read.format("orc").option("Header",True).load("/FileStore/tables/parquet_file")
df4.display()

# COMMAND ----------

df5 = spark.read.option("header",True).option("inferschema",True).csv("/FileStore/tables/orders.csv")

# COMMAND ----------

df5.display()

# COMMAND ----------

df5.printSchema()

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables")

# COMMAND ----------

df = spark.read.option("header",True).option("Inferschema",True).csv("/FileStore/tables/orders.csv")