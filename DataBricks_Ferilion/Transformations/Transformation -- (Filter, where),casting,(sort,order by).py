# Databricks notebook source
# from pyspark.sql.functions import col
from pyspark.sql.functions import *
from pyspark.sql.types import *
#col for column

# COMMAND ----------

Order_filter_df = spark.read.option("Header", True).csv("/FileStore/tables/orders.csv")

# COMMAND ----------

bigmart_filter_df = spark.read.option("header",True).csv("/FileStore/tables/BigMartSales.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC #filter/where
# MAGIC
# MAGIC

# COMMAND ----------

# df2.filter(col("Item_Fat_Content")== "Regular").display()

bigmart_filter_df.filter((col("Item_Fat_Content") == "Regular") & (col("Item_Type") == "Soft Drinks")).display()

# COMMAND ----------

bigmart_filter_df.where((col("Outlet_Location_Type").isin("Tier 2","Tier 1")) & (col("Outlet_Size").isNotNull())).display()

# COMMAND ----------

bigmart_filter_df2 = bigmart_filter_df.filter(col("Item_Type") == "Soft Drinks")
# bigmart_filter_df2.display()
bigmart_filter_df2.select(col("Item_Type"),col("Item_mrp")).limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Type casting
# MAGIC converting one datatype to another
# MAGIC

# COMMAND ----------

bigmart_filter_df2.select("*").limit(1).display()

# COMMAND ----------

bigmart_filter_df3 = bigmart_filter_df2.withColumn("item_weight",col("Item_weight").cast(IntegerType()))
bigmart_filter_df3.printSchema()

# COMMAND ----------

bigmart_sort_df = bigmart_filter_df
bigmart_sort_df.display()

# COMMAND ----------

bigmart_sort_df1 = bigmart_sort_df.sort(col("item_weight").desc())
bigmart_sort_df1.display()

# COMMAND ----------

# bigmart_sort_df2 = bigmart_sort_df.filter((col("item_type")== "Soft Drinks") & (col("Item_Fat_Content")== "Regular"))\
#     .sort(col("item_weight").desc())


bigmart_sort_df2 = bigmart_sort_df.filter((col("item_type")== "Soft Drinks") & (col("Item_Fat_Content")== "Regular"))\
    .orderBy(col("item_weight").desc())
   
bigmart_sort_df2.display()

# COMMAND ----------

bigmart_sort_df3 = bigmart_sort_df.sort(
    ["item_weight","Item_Visibility"], ascending = [0,0]
)

# COMMAND ----------

