# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

display(dbutils.fs.ls("/FileStore/tables"))

# COMMAND ----------

group_df = spark.read.option("Header",True).option("inferschema",True).csv("/FileStore/tables/classic_data/customers.csv")
group_df.select("*").limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Group by
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Sum
# MAGIC

# COMMAND ----------

group_df1  = group_df.groupBy("city").agg(sum(col("creditlimit")).alias("Total_Credit_Limit"))
display(group_df1)

# COMMAND ----------

# MAGIC %md
# MAGIC avg
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC we apply mutliple group by
# MAGIC we apply mutplie agg functios at a time
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##collect- list ====== json_arrayAgg

# COMMAND ----------

group_df2 = group_df.groupBy("country").agg(collect_list("city"))
display(group_df2)

# COMMAND ----------

# MAGIC %md
# MAGIC ##collect-set ====== json_arrayAgg == same reponse as array but remove dup

# COMMAND ----------

group_df3 = group_df.groupBy("country").agg(collect_set("city"))
display(group_df3)

# COMMAND ----------

# MAGIC %md
# MAGIC #pivot ==== rows to columns

# COMMAND ----------

