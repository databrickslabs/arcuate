# Databricks notebook source
# MAGIC %pip install -r ../requirements.txt

# COMMAND ----------

model_name = "income-prediction-model"

# COMMAND ----------

from delta_sharing_mlflow import ArcuateMagic

ip = get_ipython()
print("Adding Magic to support %python %%arcuate_export_model")
ip.register_magics(ArcuateMagic)

# COMMAND ----------

# MAGIC %sql
# MAGIC create catalog if not exists vuongnguyen

# COMMAND ----------

# MAGIC %%arcuate_export_model
# MAGIC create share 'ml_sharing' with table 'vuongnguyen.default.delta_sharing_ml_model' from model 'income-prediction-model'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM vuongnguyen.default.delta_sharing_ml_model
