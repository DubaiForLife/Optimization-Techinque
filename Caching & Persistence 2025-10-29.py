# Databricks notebook source
# MAGIC %md
# MAGIC ## Caching & Persistence
# MAGIC ### Spark can store the dataframe's partition in memory or disk for faster execution. Persisting in (MEMORY AND DISK) = Cache

# COMMAND ----------

spark.conf.set('spark.sql.adaptive.enabled','false')

# COMMAND ----------

spark.conf.get('spark.sql.adaptive.enabled')

# COMMAND ----------

df1 = spark.read.format('parquet').load('/FileStore/OrdersNewOptim/')

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
df2 = df1.withColumn('OrderID',df1.ORDERID.cast(IntegerType()))\
            .withColumn('UserID',df1.USERID.cast(IntegerType()))\
            .drop(df1.DATE_).cache()


# COMMAND ----------

df2.rdd.getNumPartitions()

# COMMAND ----------

df3 = df2.filter(df2.Year==2023)
display(df3)

# COMMAND ----------

df4 = df2.filter(df2.Year==2021)
display(df4)

# COMMAND ----------

