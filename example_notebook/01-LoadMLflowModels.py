# Databricks notebook source
# MAGIC %pip install -r ../requirements.txt

# COMMAND ----------

#experiment_name = "/Users/milos.colic@databricks.com/databricks_automl/Churn_auto_ml_data-2021_10_20-08_40"
experiment_name = "/Users/vuong.nguyen+uc@databricks.com/databricks_automl/22-01-25-16:05-automl-classification-example-4b509c14/automl-classification-example-Experiment-4b509c14"
table_name = "vuongnguyen.default.delta_sharing_ml"

# COMMAND ----------

from delta_sharing_mlflow import ArcuateMagic

ip = get_ipython()
print("Adding Magic to support %python %%arcuate_export")
ip.register_magics(ArcuateMagic)

# COMMAND ----------

# MAGIC %sql
# MAGIC create catalog if not exists vuongnguyen

# COMMAND ----------

# MAGIC %%arcuate_export 
# MAGIC create share 'ml_sharing' with table 'vuongnguyen.default.delta_sharing_ml' from experiment '/Users/vuong.nguyen+uc@databricks.com/databricks_automl/22-01-25-16:05-automl-classification-example-4b509c14/automl-classification-example-Experiment-4b509c14'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vuongnguyen.default.delta_sharing_ml
