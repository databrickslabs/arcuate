# Databricks notebook source
# MAGIC %pip install -r ../requirements.txt

# COMMAND ----------

from arcuate import ArcuateMagic

# COMMAND ----------

# MAGIC %sql
# MAGIC create catalog if not exists vuongnguyen

# COMMAND ----------

# MAGIC %%arcuate
# MAGIC create share 'ml_sharing' with table 'vuongnguyen.default.delta_sharing_ml_model' from model 'income-prediction-model'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM vuongnguyen.default.delta_sharing_ml_model
