# Databricks notebook source

import os

path = r"\Users\Dell\Desktop\Azure data\DataBricks\orders.csv"
if os.path.exists(path) : 
    print(True)
else :
    print(False)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC exists\
# MAGIC reading - options,schema\
# MAGIC select  - selct all, limit\
# MAGIC alias\
# MAGIC update - with columnrenamed,
# MAGIC withcolumn --- column name , columnvalue,add column,lit(),regexp_replace()
# MAGIC cast\
# MAGIC filter/where\
# MAGIC sort/order - single sort, multple sort
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

