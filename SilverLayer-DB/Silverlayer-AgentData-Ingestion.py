# Databricks notebook source
df = spark.sql("select * from bronzelayer.agent where merger_flg= false")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Remove all rows where Branch id not exist in Branch table </b>

# COMMAND ----------

df_branch = spark.sql("select * from bronzelayer.branch")
df_result = spark.sql("select agent.* from bronzelayer.agent inner join bronzelayer.branch on agent.branch_id=branch.branch_id where agent.merger_flg=false")
display(df_result)

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Removing records having junk phone number </b>

# COMMAND ----------

from pyspark.sql.functions import length,col
df_phone = df_result.filter(length(col("agent_phone"))==10)
display(df_phone)


# COMMAND ----------

# MAGIC %md
# MAGIC <b>Replacing the NULL value to azure@admin.com </b>

# COMMAND ----------

df_email = df_phone.fillna({'agent_email':'azure@admin.com'})
display(df_email)

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Replacing blank value to azure@admin.com</b>

# COMMAND ----------

df = spark.sql("select a.agent_id,a.agent_name,a.agent_email,a.agent_phone,a.branch_id,a.create_timestamp,a.merger_flg from bronzelayer.agent a inner join bronzelayer.branch b on a.branch_id=b.branch_id where a.merger_flg=false and length(a.agent_phone) == 10 ")
df.createOrReplaceTempView("agent_temp")

# COMMAND ----------

extension_df3 = spark.sql("select a.agent_id,a.agent_name,a.agent_phone,a.branch_id,a.create_timestamp,case when agent_email='' then 'admin@azure.com' else agent_email end as agent_email from agent_temp a")
display(extension_df3)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
df_final = extension_df3.withColumn("merged_timestamp",current_timestamp())
display(df_final)

# COMMAND ----------

extension_df3.createOrReplaceTempView("clean_agent")
spark.sql("MERGE into silverlayer.agent as T using clean_agent as s on t.agent_id=s.agent_id when matched then update set t.agent_phone=s.agent_phone,t.agent_email=s.agent_email,t.agent_name=s.agent_name,t.branch_id=s.branch_id,t.created_timestamp=s.create_timestamp,t.merged_timestamp = current_timestamp() when not matched then insert(agent_phone,agent_email,agent_name,branch_id,created_timestamp,merged_timestamp,agent_id) values (s.agent_phone,s.agent_email,s.agent_name,s.branch_id,s.create_timestamp,current_timestamp(),s.agent_id)")

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Once date is loaded into silver table. Updating the bronze layer merger flag</b>

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select * from silverlayer.agent;
# MAGIC -- update bronzelayer.agent set merger_flg=True where merger_flg=False;
# MAGIC select * from bronzelayer.agent;
