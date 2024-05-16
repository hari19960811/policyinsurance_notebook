# Databricks notebook source
# MAGIC %sql
# MAGIC create temp view vw_gold_claims_by_policy_type_and_status 
# MAGIC AS
# MAGIC select
# MAGIC   policy_type,
# MAGIC   claim_status,
# MAGIC   count(*) as total_claims,
# MAGIC   sum(claim_amount) as total_claim_amount
# MAGIC from
# MAGIC   silverlayer.claim c
# MAGIC   join silverlayer.policy p
# MAGIC     on c.policy_id=p.policy_id
# MAGIC group by
# MAGIC   policy_type,
# MAGIC   claim_status having p.policy_type is not null;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vw_gold_claims_by_policy_type_and_status;

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into goldlayer.claims_by_policy_type_and_status as t using vw_gold_claims_by_policy_type_and_status as s on t.policy_type=s.policy_type and t.claim_status=s.claim_status when matched then update set t.total_claim_mount=s.total_claim_amount,t.total_claims=s.total_claims,t.updated_timestamp=current_timestamp() when not matched then insert(policy_type,  claim_status,total_claims,total_claim_mount,updated_timestamp) values(s.policy_type, s.claim_status,s.total_claims,s.total_claim_amount,current_timestamp())

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from goldlayer.claims_by_policy_type_and_status;

# COMMAND ----------

# MAGIC %sql
# MAGIC create temporary view vw_gold_claims_analysis 
# MAGIC as
# MAGIC select 
# MAGIC   policy_type,
# MAGIC   avg(claim_amount) as avg_claim_amount,
# MAGIC   max(claim_amount) as max_claim_amount,
# MAGIC   min(claim_amount) as min_claim_amount,
# MAGIC   count(distinct claim_id) as total_claims
# MAGIC from 
# MAGIC   silverlayer.claim c
# MAGIC   join silverlayer.policy p  on c.policy_id=p.policy_id
# MAGIC   group by 
# MAGIC     policy_type having p.policy_type is not null; 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vw_gold_claims_analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into goldlayer.claims_analysis as t using vw_gold_claims_analysis as s on t.policy_type=s.policy_type when matched then update set t.avg_claim_amount=s.avg_claim_amount,t.max_claim_amount=s.max_claim_amount,t.min_claim_integer=s.min_claim_amount,t.total_claims=s.total_claims,t.update_timestamp=current_timestamp() when not matched then insert (avg_claim_amount,max_claim_amount,min_claim_integer,total_claims,update_timestamp,policy_type)values(s.avg_claim_amount,s.max_claim_amount,s.min_claim_amount,s.total_claims,current_timestamp(),s.policy_type)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from goldlayer.claims_analysis
