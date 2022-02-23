# Databricks notebook source
# MAGIC %md # AutoML classification example
# MAGIC
# MAGIC ## Requirements
# MAGIC Databricks Runtime for Machine Learning 8.3 or above.

# COMMAND ----------

# MAGIC %md ## Census income dataset
# MAGIC This dataset contains census data from the 1994 census database. Each row represents a group of individuals. The goal is to determine whether a group has an income of over 50k a year or not. This classification is represented as a string in the **income** column with values `<=50K` or `>50k`.

# COMMAND ----------

from pyspark.sql.types import DoubleType, StringType, StructType, StructField

schema = StructType(
    [
        StructField("age", DoubleType(), False),
        StructField("workclass", StringType(), False),
        StructField("fnlwgt", DoubleType(), False),
        StructField("education", StringType(), False),
        StructField("education_num", DoubleType(), False),
        StructField("marital_status", StringType(), False),
        StructField("occupation", StringType(), False),
        StructField("relationship", StringType(), False),
        StructField("race", StringType(), False),
        StructField("sex", StringType(), False),
        StructField("capital_gain", DoubleType(), False),
        StructField("capital_loss", DoubleType(), False),
        StructField("hours_per_week", DoubleType(), False),
        StructField("native_country", StringType(), False),
        StructField("income", StringType(), False),
    ]
)
input_df = (
    spark.read.format("csv")
    .schema(schema)
    .load("/databricks-datasets/adult/adult.data")
)

# COMMAND ----------

# MAGIC %md ## Train/test split

# COMMAND ----------

train_df, test_df = input_df.randomSplit([0.99, 0.01], seed=42)
display(train_df)

# COMMAND ----------

# MAGIC %md # Training
# MAGIC The following command starts an AutoML run. You must provide the column that the model should predict in the `target_col` argument.
# MAGIC When the run completes, you can follow the link to the best trial notebook to examine the training code. This notebook also includes a feature importance plot.

# COMMAND ----------

from databricks import automl

summary = automl.classify(train_df, target_col="income", timeout_minutes=30)

# COMMAND ----------

# MAGIC %md The following command displays information about the AutoML output.

# COMMAND ----------

help(summary)

# COMMAND ----------

# MAGIC %md
# MAGIC # Register best model in registry

# COMMAND ----------

import mlflow

model_uri = summary.best_trial.model_path
model_name = "income-prediction-model"

model_details = mlflow.register_model(model_uri=model_uri, name=model_name)

import time
from mlflow.tracking.client import MlflowClient
from mlflow.entities.model_registry.model_version_status import ModelVersionStatus

# Wait until the model is ready
def wait_until_ready(model_name, model_version):
    client = MlflowClient()
    for _ in range(10):
        model_version_details = client.get_model_version(
            name=model_name,
            version=model_version,
        )
        status = ModelVersionStatus.from_string(model_version_details.status)
        print("Model status: %s" % ModelVersionStatus.to_string(status))
        if status == ModelVersionStatus.READY:
            break
        time.sleep(1)


wait_until_ready(model_details.name, model_details.version)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add some more details

# COMMAND ----------

from mlflow.tracking.client import MlflowClient

client = MlflowClient()
client.update_registered_model(
    name=model_details.name,
    description="This model predicts whether a group has an income of over 50k a year or not.",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Promote the current version to Prod

# COMMAND ----------

client.transition_model_version_stage(
    name=model_details.name,
    version=model_details.version,
    stage="Production",
)

client.update_model_version(
    name=model_details.name,
    version=model_details.version,
    description="This is the production version",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register another version, and promote it to Staging

# COMMAND ----------

import mlflow

model_uri = summary.best_trial.model_path

model_details = mlflow.register_model(model_uri=model_uri, name=model_name)

wait_until_ready(model_details.name, model_details.version)

client.transition_model_version_stage(
    name=model_details.name,
    version=model_details.version,
    stage="Staging",
)

client.update_model_version(
    name=model_details.name,
    version=model_details.version,
    description="This is the staging version",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register another 5 versions

# COMMAND ----------

for _ in range(5):
    model_details = mlflow.register_model(model_uri=model_uri, name=model_name)
