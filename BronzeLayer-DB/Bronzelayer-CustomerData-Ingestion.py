# Databricks notebook source
from pyspark.sql.functions import lit

#Schema for Customer data
customerschema = "customer_id integer, first_name string, last_name string,email string , phone string,country string, \
                   city string,registration_date timestamp,date_of_birth timestamp,gender string"
df = spark.read.csv("/mnt/landing/Customer/*.csv",header=True,inferSchema=False,schema=customerschema)
df_merge_flg = df.withColumn("merge_flg",lit(False))
df_merge_flg.write.option("path","/mnt/bronzelayer/CustomerData/").mode("append").saveAsTable("bronzelayer.customer")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronzelayer.customer

# COMMAND ----------

from datetime import datetime

#Curdate value
curdate = datetime.now().strftime("%m-%d-%Y")
newpath = "/mnt/processed/Customer"+'/'+curdate
filepath = "/mnt/landing/Customer/"
dbutils.fs.mv(filepath,newpath,True)
