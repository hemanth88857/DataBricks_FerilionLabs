# Databricks notebook source
dbutils.fs.ls("/FileStore/tables")

# COMMAND ----------

df1 = spark.read.option("Header",True).option("Inferschema",True).csv("/FileStore/tables/orders.csv")
# df1.printSchema()
df1.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Change 1 datatype to another data type 
# MAGIC #using Schema

# COMMAND ----------


df1_new_schema = '''
                    order_id int,
                    customer_id int,
                    product_id string,
                    category string,
                    amount double,
                    order_date date,
                    status string
                 '''




# COMMAND ----------

df1 = spark.read.format("csv")\
                .schema(df1_new_schema)\
                 .option("Header",True)\
                     .option("inferschema",True)\
                         .load("/FileStore/tables/orders.csv")


# COMMAND ----------

df1.printSchema()

# COMMAND ----------

# MAGIC %md

# COMMAND ----------

# MAGIC %md
# MAGIC Change 1 datatype to another data type
# MAGIC ## using struct type
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df1_strct_schema = StructType([
   StructField("order_id" ,IntegerType(),True),
                                  StructField("customer_id",IntegerType(),True),
                                 StructField("product_id",StringType(),True),
                                StructField("category",StringType(),True),
                                 StructField("amount", DoubleType(), True),
                                   StructField("order_date", DateType(), True),
                                   StructField("status", StringType(), True)
                                   ])

# COMMAND ----------

df1 = spark.read.option("Header",True).schema(df1_strct_schema).csv("/FileStore/tables/orders.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC pratice of schema on 09/06/2025

# COMMAND ----------

df_new_schema1 = '''
                    order_id int,
                    customer_id int ,
                    product_id int,
                    category string,
                    amount double,
                    order_date date,
                    status string 
'''

# COMMAND ----------

df1 = spark.read.option("header",True).schema(df_new_schema1).csv("/FileStore/tables/orders.csv")

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

df_new_schema2 = StructType([
                    StructField('order_id',IntegerType(),True),
                    StructField('customer_id',IntegerType(),True),
                    StructField('product_id',IntegerType(),True),
                    StructField('category',StringType(),True),
                    StructField('amount',DoubleType(),True),
                    StructField('order_date',DateType(),True),
                    StructField('status',StringType(),True),


])

# COMMAND ----------

df1 = spark.read.option("header",True).schema(df_new_schema2).csv("/FileStore/tables/orders.csv")


# COMMAND ----------

