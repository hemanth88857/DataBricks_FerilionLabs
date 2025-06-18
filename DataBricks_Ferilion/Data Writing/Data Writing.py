# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

# display(dbutils.fs.ls("/FileStore/tables"))
display(dbutils.fs.ls("dbfs:/FileStore/tables/Writing_Data/"))
# dbutils.fs.rm("/FileStore/tables/Writing_data",recurse = True)


# COMMAND ----------

dbutils.fs.mkdirs("/FileStore/tables/Writing_Data")

# COMMAND ----------

groupby_df = spark.read.option("Header",True).option("inferschema",True).csv("/FileStore/tables/BigMartSales.csv")
display(groupby_df)

# COMMAND ----------

groupby_df = groupby_df.withColumn("rank",rank().over(Window.partitionBy(col("Item_Type")).orderBy(col("Item_MRP").desc())))
display(groupby_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #Append

# COMMAND ----------

csv_dataWriting = groupby_df.write.format("csv").mode("append").save("/FileStore/tables/Writing_Data/bigSales_withrank.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC #overwrite

# COMMAND ----------

csv_dataWriting_overWriting = groupby_df.write.format("csv").mode("overwrite").save("/FileStore/tables/Writing_Data/bigSales_withrank.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC #error

# COMMAND ----------

csv_dataWriting_error  =groupby_df.write.format("csv").mode("error").save("/FileStore/tables/Writing_Data/bigSales_withrank.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ignore

# COMMAND ----------

csv_dataWriting_ignore  =groupby_df.write.format("csv").mode("ignore").save("/FileStore/tables/Writing_Data/bigSales_withrank.csv")

# COMMAND ----------

csv_dataWriting = groupby_df.write.format("parquet").saveAsTable("bigSales_details")

# COMMAND ----------

sql1 = spark.sql("SELECT sum((Item_MRP)),Item_Type FROM bigsales_details GROUP BY Item_Type")
display(sql1)

# COMMAND ----------

sql_df = spark.sql("select * from groupby_df")

# COMMAND ----------

