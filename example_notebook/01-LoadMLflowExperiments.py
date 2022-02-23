# Databricks notebook source
# MAGIC %pip install -r ../requirements.txt

# COMMAND ----------

experiment_name = "/Users/vuong.nguyen+uc@databricks.com/databricks_automl/22-02-23-11:12-00-GenerateAutoMLModel-9b3d7e37/00-GenerateAutoMLModel-Experiment-9b3d7e37"

# COMMAND ----------

from delta_sharing_mlflow import ArcuateMagic

# COMMAND ----------

# MAGIC %sql
# MAGIC create catalog if not exists vuongnguyen

# COMMAND ----------

# MAGIC %%arcuate_export_experiment 
# MAGIC create share 'ml_sharing' with table 'vuongnguyen.default.delta_sharing_ml_experiment' from experiment '/Users/vuong.nguyen+uc@databricks.com/databricks_automl/22-02-23-11:12-00-GenerateAutoMLModel-9b3d7e37/00-GenerateAutoMLModel-Experiment-9b3d7e37'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vuongnguyen.default.delta_sharing_ml_experiment
