# Databricks notebook source
# MAGIC %md
# MAGIC Tranformation

# COMMAND ----------

from pyspark.sql.functions import col
#col for column

# COMMAND ----------

df1 = spark.read.option("Header", True).csv("/FileStore/tables/orders.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ### # Select

# COMMAND ----------

#-------------------------------------------------------- Select all--------------------------------------------------
# display(df1) # select display all
# display(df1.select("*")) # select all 


# df1.select("order_id").display()
#df1.select(col('order_id'),col('category')).display() -- standard


#df1.select("*").display()
# df1.select(col("order_id")).limit(10).display()
# display(df1.select(col("order_id")).limit(10))


# df1.select(col("order_id"),col("category")).display()
# df1.select(col("order_id")).limit(10).display()
df1.select("*").display()
#display(df1.select(col("order_id")).distinct().limit(10)) -- distinct 


# COMMAND ----------

# MAGIC %md
# MAGIC ##Alias
# MAGIC

# COMMAND ----------

df1.select(col("order_id").alias("Order_ID")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### # with column renamed -- it will rename the column permanently in [DF](url)

# COMMAND ----------

df1.withColumnRenamed("order_id","Ord_ID")
df1.printSchema
# df1.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### # with column -- add new column
# MAGIC lit -- add value to new colum while creating
# MAGIC

# COMMAND ----------

# df3 = df1.withColumn("New_data",lit("null"))
display(df3.select("*"))


# COMMAND ----------

# MAGIC %md
# MAGIC with column -- regexp_replace --     to replace the data in column
# MAGIC

# COMMAND ----------

df3 = df3.withColumn("New_data",regexp_replace(col("New_data"),"null","New_Column")) 