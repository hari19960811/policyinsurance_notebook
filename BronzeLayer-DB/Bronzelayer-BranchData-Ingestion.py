# Databricks notebook source
from pyspark.sql.functions import lit

branch_schema = "branch_id integer,branch_country string,branch_city string"
df = spark.read.parquet("/mnt/landing/BranchData/*.parquet",inferSchema=False,schema=branch_schema)
df_with_flg = df.withColumn("merge_flg",lit(False))
df_with_flg.write.option("path","/mnt/bronzelayer/BranchData/").mode("append").saveAsTable("bronzelayer.Branch")

# COMMAND ----------

# MAGIC %sql
# MAGIC select* from bronzelayer.branch

# COMMAND ----------

from datetime import datetime

currenttime = datetime.now().strftime('%m-%d-%Y')
newpath = "/mnt/processed/BranchData/" + currenttime + "/"
dbutils.fs.mv("/mnt/landing/BranchData/dbo.Branch.parquet",newpath)
