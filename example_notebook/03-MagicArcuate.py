# Databricks notebook source
# MAGIC %pip install -r ../requirements.txt

# COMMAND ----------

import delta_sharing_mlflow

# COMMAND ----------

query = 'create share `shr  s123` as select experiment `exp e123`'
delta_sharing_mlflow.arcuate_parse(query)

# COMMAND ----------

query = 'create EXPERIMENT `exp e123` as select * from `shr s123`'
delta_sharing_mlflow.arcuate_parse(query)
