# Databricks notebook source
experiment_name = "/Users/milos.colic@databricks.com/databricks_automl/Churn_auto_ml_data-2021_10_20-08_40"

# COMMAND ----------

from mlflow.tracking import MlflowClient

# COMMAND ----------

client = MlflowClient()

# COMMAND ----------

experiment = client.get_experiment_by_name(experiment_name)

# COMMAND ----------

experiment

# COMMAND ----------

experiment.

# COMMAND ----------

display(
  client.list_run_infos(experiment.experiment_id)
)

# COMMAND ----------

import mlflow

experiment_infos = mlflow.search_runs(experiment.experiment_id)

# COMMAND ----------

experiment_infos_df = spark.createDataFrame(experiment_infos)

# COMMAND ----------

display(experiment_infos_df)

# COMMAND ----------

experiment_infos_df.printSchema()

# COMMAND ----------

metrics_subschema = [cn for cn in experiment_infos_df.columns if "metrics." in cn]
params_subschema  = [cn for cn in experiment_infos_df.columns if "params." in cn]
tags_subschema    = [cn for cn in experiment_infos_df.columns if "tags." in cn]
run_info_subschema = [cn for cn in experiment_infos_df.columns if cn not in tags_subschema + params_subschema + metrics_subschema]

# COMMAND ----------

experiment_infos_df.schema.fields[0].name

# COMMAND ----------

experiment_infos_df.select(F.col("`metrics.training_precision_score`"))

# COMMAND ----------

experiment_infos_df["metrics.training_recall_score"]

# COMMAND ----------

from pyspark.sql import functions as F

normalized = experiment_infos_df.select(
  F.struct(*[F.col(cn) for cn in run_info_subschema]).alias("run_info"),
  F.map_from_arrays(
    F.array(*[F.lit(cn.replace("metrics.", "")) for cn in metrics_subschema]),
    F.array(*[F.col(f"`{cn}`") for cn in metrics_subschema])
  ).alias("metrics"),
  F.map_from_arrays(
    F.array(*[F.lit(cn.replace("params.", "")) for cn in params_subschema]),
    F.array(*[F.col(f"`{cn}`") for cn in params_subschema])
  ).alias("params"),
  F.map_from_arrays(
    F.array(*[F.lit(cn.replace("tags.", "")) for cn in tags_subschema]),
    F.array(*[F.col(f"`{cn}`") for cn in tags_subschema])
  ).alias("tags")
)

# COMMAND ----------

display(normalized)

# COMMAND ----------


