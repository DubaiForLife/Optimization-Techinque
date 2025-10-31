# Databricks notebook source
# MAGIC %md
# MAGIC ### Partition has to be made on the joining key in the fact table and apply joining-key filter before joining to the dimension

# COMMAND ----------

spark.conf.set('spark.sql.adaptive.enabled','false')
spark.conf.set('spark.sql.optimizer.dynamicPartitionPruning.enabled','false')
spark.conf.set('spark.sql.autoBroadcastJoinThreshold',-1)

# COMMAND ----------

spark.conf.get('spark.sql.optimizer.dynamicPartitionPruning.enabled')

# COMMAND ----------

df_order_opt.write.format('parquet').mode('overwrite').partitionBy("BRANCH_ID").option('path','/FileStore/OrdersBranchopt').save()

# COMMAND ----------

df_order_branch_opt = spark.read.format('parquet').load('/FileStore/OrdersBranchopt/')

# COMMAND ----------

df_branch = spark.read.format('csv').option('header','True').option('sep',';').load('dbfs:/FileStore/Branches_ENG.csv')

# COMMAND ----------

df_join_new = df_order_branch_opt.join(df_branch.filter(df_branch.BRANCH_ID=='538-KA1'),df_order_branch_opt.BRANCH_ID == df_branch.BRANCH_ID,"inner")

# COMMAND ----------

display(df_join_new.filter(df_join_new.REGION=='Ic Anadolu'))

# COMMAND ----------

