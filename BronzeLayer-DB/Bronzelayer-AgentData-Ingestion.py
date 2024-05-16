# Databricks notebook source

from pyspark.sql.functions import lit
schema = "agent_id integer , agent_name string , agent_email string , agent_phone string , branch_id integer , created_timestamp timestamp"
df =spark.read.parquet("/mnt/landing/AgentData/dbo.Agent.parquet")
df_with_flag = df.withColumn("merger_flg",lit(False))
df_with_flag.write.option("path","/mnt/bronzelayer/Agent").mode("append").saveAsTable("bronzelayer.Agent")


# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/landing/AgentData/

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronzelayer.agent;

# COMMAND ----------

from datetime import datetime

#Get the current date time in the format mm-dd-yyyy format
current_time = datetime.now().strftime('%m-%d-%y')

# print the current time
print(current_time)

new_folder = "/mnt/processed/AgentData/"+current_time
dbutils.fs.mv("/mnt/landing/AgentData/",new_folder,True)

