# Databricks notebook source
display(dbutils.fs.ls("/FileStore/tables/emp_data/"))

# COMMAND ----------

empB_df1 = spark.read.option("Header",True).option("inferschema",True).csv("/FileStore/tables/emp_data/Employees_BranchB.csv")
empA_df1 = spark.read.option("Header",True).option("inferschema",True).csv("/FileStore/tables/emp_data/Employees_BranchA.csv")


# COMMAND ----------

empA_df1.select("*").display()

# COMMAND ----------

# MAGIC %md
# MAGIC #union
# MAGIC

# COMMAND ----------

union_df1 = empA_df1.union(empB_df1)

# COMMAND ----------

display(union_df1)

# COMMAND ----------

# MAGIC %md
# MAGIC #UnoinByName

# COMMAND ----------

unionbyname_df1 = empA_df1.unionByName(empB_df1)

# COMMAND ----------

display(unionbyname_df1)

# COMMAND ----------

