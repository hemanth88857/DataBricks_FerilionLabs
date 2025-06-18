# Databricks notebook source

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

display(dbutils.fs.ls("/FileStore/tables/"))

# COMMAND ----------

when_df = spark.read.option("Header",True).option("inferschema",True).csv("/FileStore/tables/BigMartSales.csv")
when_df.select("*").limit(5).display()

# COMMAND ----------

when_df1 = when_df.withColumn("Veg/Non-Veg",lit("Null"))

# COMMAND ----------

display(when_df1)

# COMMAND ----------

# MAGIC %md
# MAGIC we can write one when condtion with other wise

# COMMAND ----------

when_df2 = when_df1.withColumn("veg/Non-veg",when(col("item_type") == "Meat",'Non-Veg').otherwise("Veg"))
display(when_df2)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC we can write one when  with (&)condtion with other wise

# COMMAND ----------

when_df3 = when_df2.withColumn("MostOrdered",when((col("Veg/Non-Veg")== "Veg") & (col("Item_MRP") > 100),'5').otherwise("3"))
display(when_df3)

# COMMAND ----------

# MAGIC %md
# MAGIC we can write more then one  when  with (&)condtion with other wise

# COMMAND ----------

