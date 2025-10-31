# Databricks notebook source
# MAGIC %md
# MAGIC ### While performing shuffling operations , AQE can coalesce (reduce) the number of partitions from 200

# COMMAND ----------

spark.conf.set('spark.sql.adaptive.enabled','true')

# COMMAND ----------

df1 = spark.read.format('parquet').load('/FileStore/Orders/')

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
df2 = df1.withColumn('OrderID',df1.ORDERID.cast(IntegerType()))\
            .withColumn('UserID',df1.USERID.cast(IntegerType()))\
            .drop(df1.DATE_)


# COMMAND ----------

df2.rdd.getNumPartitions()

# COMMAND ----------

df3 = df2.groupBy(df2.NAMESURNAME).agg(sum(df2.TotalBasket)).alias('Total')

# COMMAND ----------

df3.rdd.getNumPartitions()

# COMMAND ----------

df3.write.format('parquet').mode('overwrite').option('path','/FileStore/AQE').save()

# COMMAND ----------

