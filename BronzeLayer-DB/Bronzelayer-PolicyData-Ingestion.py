# Databricks notebook source
from pyspark.sql.functions import lit
#Policy Schema
policyschema = "policy_id integer,policy_type string,customer_id integer,start_date timestamp,end_date timestamp,premium double ,coverage_amount double"
df = spark.read.json("/mnt/landing/PolicyData/*.json",schema=policyschema)
df_merge_flg = df.withColumn("merge_flag",lit(False))
df_merge_flg.write.option("path","/mnt/bronzelayer/PolicyData/").mode("append").saveAsTable("bronzelayer.policy")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronzelayer.policy

# COMMAND ----------

from datetime import datetime

#Curdate value
curdate = datetime.now().strftime("%m-%d-%Y")
newpath = "/mnt/processed/PolicyData"+'/'+curdate
filepath = "/mnt/landing/PolicyData/"
dbutils.fs.mv(filepath,newpath,True)
