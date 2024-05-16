# Databricks notebook source
# MAGIC %md
# MAGIC <b>Remove all the rows where customer id,Policy ID is null </b>

# COMMAND ----------

df = spark.sql("select * from bronzelayer.policy where customer_id is not null and policy_id is not null and merge_flag=false")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Remove all rows where Customer id not exist in Customer table </b>

# COMMAND ----------

df_agent = spark.sql("select p.* from bronzelayer.policy p join bronzelayer.customer c on p.customer_id=c.customer_id where p.customer_id is not null and p.policy_id is not null and p.merge_flag=false")
display(df_agent)

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Every policy must have premium and coverage amount >0 </b>

# COMMAND ----------

df_valid = spark.sql("select p.* from bronzelayer.policy p join bronzelayer.customer c on p.customer_id=c.customer_id where p.customer_id is not null and p.policy_id is not null and p.merge_flag=false and p.premium > 0 and p.coverage_amount >0")
display(df_valid)

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Validate end_date > start_date</b>

# COMMAND ----------

df_final = spark.sql("select p.* from bronzelayer.policy p join bronzelayer.customer c on p.customer_id=c.customer_id where p.customer_id is not null and p.policy_id is not null and p.merge_flag=false and p.premium > 0 and p.coverage_amount >0 and end_date > start_date")
display(df_final)

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Merged the table with merged_date timestamp as current Timestamp </b>

# COMMAND ----------

df_final.createOrReplaceTempView("clean_policy")
spark.sql("merge into silverlayer.policy as t using clean_policy as s on t.policy_id=s.policy_id when matched then update set t.policy_type=s.policy_type,t.customer_id=s.customer_id,t.start_date=s.start_date,t.end_date=s.end_date,t.premium=s.premium,t.coverage_amount=s.coverage_amount,t.merged_timestamp=current_timestamp() when not matched then insert(policy_type,customer_id,start_date,end_date,premium,coverage_amount,merged_timestamp,policy_id) values(s.policy_type,s.customer_id,s.start_date,s.end_date,s.premium,s.coverage_amount,current_timestamp(),s.policy_id)")

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Update the merge flag in the Bronze layer</b>

# COMMAND ----------

# MAGIC %sql
# MAGIC update bronzelayer.policy set merge_flag=true where merge_flag=false;
