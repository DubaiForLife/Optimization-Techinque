# Databricks notebook source
# MAGIC %md
# MAGIC ### Check ADE status

# COMMAND ----------

spark.conf.get('spark.sql.adaptive.enabled')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Enable/Disable AQE

# COMMAND ----------

spark.conf.set('spark.sql.adaptive.enabled','false')

# COMMAND ----------


df = spark.read.option('header','true').csv(path = 'dbfs:/FileStore/Orders.csv')



# COMMAND ----------

# MAGIC %md
# MAGIC ### Get number of partitions

# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get partition size

# COMMAND ----------

spark.conf.get('spark.sql.files.maxPartitionBytes')

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ### Changing the partition size to 128KB

# COMMAND ----------

spark.conf.set('spark.sql.files.maxPartitionBytes',131072)
#128*1024*

# COMMAND ----------

# MAGIC %md
# MAGIC ### Changing to default partition size 128MB

# COMMAND ----------

spark.conf.set('spark.sql.files.maxPartitionBytes',134217728)
#128*1024*1024

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
df1= df.withColumn("partitionid",spark_partition_id()).withColumn('TotalBasket',regexp_replace('TotalBasket',',','.').cast(DecimalType(10,2)))\
    .withColumn('Date',to_date('DATE_','yyyy-MM-dd HH:mm:ss'))
df1.groupBy(col('partitionid')).agg(count('*')).display()

# COMMAND ----------

df1.rdd.getNumPartitions()

# COMMAND ----------

df = df.repartition(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Writing into 10 parquet files

# COMMAND ----------

df1.write.format('parquet').mode('overwrite').option('path','/FileStore/Orders').save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading from 10 parquet files

# COMMAND ----------

df_new = spark.read.format('parquet').load('/FileStore/Orders/')
display(df_new)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scanning optimization - Static Data pruning - partitionBy
# MAGIC ### Instead of doing a full scan of the table/file , the data is partitioned by a KEY field and written to a storage so when reading the data , spark will scan only the required partition hence avoiding full scan. It reduces the amount of data read and processed

# COMMAND ----------

df_new.write.format('parquet').mode('overwrite').partitionBy("Year").option('path','/FileStore/OrdersNewOptim').save()

# COMMAND ----------

df_order_opt = spark.read.format('parquet').load('/FileStore/OrdersNewOptim/')

# COMMAND ----------

display(df_new.filter(df_new.Year==2023))
--Rows read = 45,046

# COMMAND ----------

display(df_order_opt.filter(df_order_opt.Year==2023))
--Rows read = 12,048

# COMMAND ----------

# MAGIC %md
# MAGIC ## BroadCast join - to avoid shuffling
# MAGIC ### When joining bigger table with smaller one , the data from small table is broadcasted to all partitions so that there's no need for shufflling

# COMMAND ----------

df_cus = spark.read.option('header','true').option('sep',';').csv('dbfs:/FileStore/Customers_ENG.csv')
display(df_cus)

# COMMAND ----------

df_join = df_order_opt.join(df_cus,df_order_opt.USERID == df_cus.USERID,'left')

# COMMAND ----------

display(df_join)

# COMMAND ----------

from pyspark.sql.functions import *
df_join_opt = df_order_opt.join(broadcast(df_cus),df_order_opt.USERID == df_cus.USERID,'left')

# COMMAND ----------

display(df_join_opt)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Broadcast using SQL

# COMMAND ----------

df_cus.createOrReplaceTempView('customer')
df_new.createOrReplaceTempView('orders')

# COMMAND ----------

df_sql=spark.sql(
'''select /*+Broadcast(c)*/
o.* from orders o
join customer c on c.USERID = o.USERID'''
)

# COMMAND ----------

display(df_sql)

# COMMAND ----------

