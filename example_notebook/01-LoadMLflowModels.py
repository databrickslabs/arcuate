# Databricks notebook source
# MAGIC %pip install -r requirements.txt

# COMMAND ----------

#experiment_name = "/Users/milos.colic@databricks.com/databricks_automl/Churn_auto_ml_data-2021_10_20-08_40"
experiment_name = "/Users/vuong.nguyen+uc@databricks.com/databricks_automl/22-01-25-16:05-automl-classification-example-4b509c14/automl-classification-example-Experiment-4b509c14"
table_name = "vuongnguyen.default.delta_sharing_ml"

# COMMAND ----------

from delta_sharing_mlflow import normalize_mlflow_df

# COMMAND ----------

from mlflow.tracking import MlflowClient
import mlflow

client = MlflowClient()
experiment = client.get_experiment_by_name(experiment_name)
experiment_infos = mlflow.search_runs(experiment.experiment_id, filter_string="tags.mlflow.runName != 'Training Data Storage and Analysis'")
experiment_infos_df = spark.createDataFrame(experiment_infos)

# COMMAND ----------

display(experiment_infos_df)

# COMMAND ----------

normalized = normalize_mlflow_df(experiment_infos_df)

# COMMAND ----------

display(normalized)

# COMMAND ----------

normalized.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
