# Databricks notebook source
from pyspark.sql.functions import lit

#Schema for the Claim Data
claimschema = "claim_id integer , policy_id integer , date_of_claim timestamp , claim_amount integer, claim_status string \
                lastupdatedtimestamp string "
df = spark.read.parquet("/mnt/landing/ClaimData/*.parquet",inferSchema=False,schema=claimschema)

#Adding a cloumn of flag
df_merge_flag = df.withColumn("flag",lit(False))
df_merge_flag.write.option("path","/mnt/bronzelayer/ClaimData/").mode("append").saveAsTable("bronzelayer.claim")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronzelayer.claim

# COMMAND ----------

from datetime import datetime

#Curdate value
curdate = datetime.now().strftime("%m-%d-%Y")
newpath = "/mnt/processed/ClaimData"+'/'+curdate
filepath = "/mnt/landing/ClaimData/"
dbutils.fs.mv(filepath,newpath,True)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/landing/ClaimData/claim.parquet"
