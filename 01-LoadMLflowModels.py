# Databricks notebook source
#experiment_name = "/Users/milos.colic@databricks.com/databricks_automl/Churn_auto_ml_data-2021_10_20-08_40"
experiment_name = "/Users/vuong.nguyen+uc@databricks.com/databricks_automl/22-01-25-16:05-automl-classification-example-4b509c14/automl-classification-example-Experiment-4b509c14"
table_name = "vuongnguyen.default.delta_sharing_ml"

# COMMAND ----------

from mlpickling import pickle_artifacts_udf, pickle_model_udf

# COMMAND ----------

from mlflow.tracking import MlflowClient
import mlflow

client = MlflowClient()
experiment = client.get_experiment_by_name(experiment_name)
experiment_infos = mlflow.search_runs(experiment.experiment_id, filter_string="tags.mlflow.runName != 'Training Data Storage and Analysis'")

# COMMAND ----------

experiment_infos_df = spark.createDataFrame(experiment_infos)

# COMMAND ----------

display(experiment_infos_df)

# COMMAND ----------

metrics_subschema = [cn for cn in experiment_infos_df.columns if "metrics." in cn]
params_subschema  = [cn for cn in experiment_infos_df.columns if "params." in cn]
tags_subschema    = [cn for cn in experiment_infos_df.columns if "tags." in cn]
run_info_subschema = [cn for cn in experiment_infos_df.columns if cn not in tags_subschema + params_subschema + metrics_subschema]

# COMMAND ----------

from pyspark.sql import functions as F

normalized = (experiment_infos_df
              .select(
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
                  ).alias("tags"))
              .withColumn("model_payload", pickle_model_udf("run_info.run_id"))
              .withColumn("artifact_payload", pickle_artifacts_udf("run_info.run_id")) 
             )

# COMMAND ----------

display(normalized)

# COMMAND ----------

normalized.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
