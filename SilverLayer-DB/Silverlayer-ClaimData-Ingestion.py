# Databricks notebook source
# MAGIC %md
# MAGIC <b>Remove all where claim_id,policy_id,claim_status,last_updated null </b>

# COMMAND ----------

df = spark.sql("select * from bronzelayer.claim where flag =false and claim_id is not null and policy_id is not null and claim_status is not null and LastUpdatedTimeStamp is not null")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Remove all rows where policy_id not exist in policy table</b>

# COMMAND ----------

df_policy = spark.sql("select c.* from bronzelayer.claim c join bronzelayer.policy p on c.policy_id=p.policy_id  where c.flag =false and c.claim_id is not null and c.policy_id is not null and c.claim_status is not null and c.LastUpdatedTimeStamp is not null")
display(df_policy)

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Convert date_of_claim to Date column with format (mm-dd-yyyy)</b>

# COMMAND ----------

df_date = spark.sql("select c.claim_id,c.policy_id,date_format(c.date_of_claim,'MM-dd-yyyy'),c.claim_amount,c.claim_status,c.LastUpdatedTimeStamp from bronzelayer.claim c join bronzelayer.policy p on c.policy_id=p.policy_id  where c.flag =false and c.claim_id is not null and c.policy_id is not null and c.claim_status is not null and c.LastUpdatedTimeStamp is not null")
display(df_date)

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Ensure claim_amount is >0 </b>

# COMMAND ----------

df_claim = spark.sql("select c.claim_id,c.policy_id,date_format(c.date_of_claim,'MM-dd-yyyy') as date_of_claim,c.claim_amount,c.claim_status,c.LastUpdatedTimeStamp from bronzelayer.claim c join bronzelayer.policy p on c.policy_id=p.policy_id  where c.flag =false and c.claim_id is not null and c.policy_id is not null and c.claim_status is not null and c.LastUpdatedTimeStamp is not null and c.claim_amount > 0")
display(df_claim)

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Add the merged date_timestamp (current timestamp)</b>

# COMMAND ----------

df_claim.createOrReplaceTempView("clean_claim")
spark.sql("merge into silverlayer.claim as t using bronzelayer.claim as s on t.claim_id=s.claim_id when matched then update set t.policy_id=s.policy_id,t.date_of_claim=s.date_of_claim,t.claim_amount=s.claim_amount,t.claim_status=s.claim_status,t.lastUpdatedTimeStamp=s.LastUpdatedTimeStamp,t.merged_timestamp=current_timestamp() when not matched then insert (policy_id,date_of_claim,claim_amount,claim_status,lastUpdatedTimeStamp,merged_timestamp,claim_id) values(s.policy_id,s.date_of_claim,s.claim_amount,s.claim_status,s.lastUpdatedTimeStamp,current_timestamp(),s.claim_id)")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silverlayer.claim

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Update the flag in the Bronzelayer.claim table </b>

# COMMAND ----------

# MAGIC %sql
# MAGIC update bronzelayer.claim set flag=true where flag=false;
