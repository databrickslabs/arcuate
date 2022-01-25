# Databricks notebook source
experiment_id = "3266293086700303"
table_name = "vuongnguyen.default.delta_sharing_ml"

# COMMAND ----------

import shutil, os
import pickle
import mlflow
from mlflow.tracking import MlflowClient
from pyspark.sql.functions import udf
from pyspark.sql.types import BinaryType, MapType, StringType

# COMMAND ----------

@udf(returnType=BinaryType()) 
def pickled_model(model_path: str) -> bytes:
    return pickle.dumps(mlflow.pyfunc.load_model(model_path))

# COMMAND ----------

@udf(returnType=MapType(StringType(), BinaryType()))
def pickled_artifacts(run_id: str)-> dict:
    client = MlflowClient()
    artifacts = client.list_artifacts(run_id)
    if len(artifacts) > 0: # Because of https://github.com/mlflow/mlflow/issues/2839
        local_dir = "/tmp/artifact_downloads"
        if os.path.exists(local_dir):
            shutil.rmtree(local_dir)
        os.mkdir(local_dir)
        local_path = client.download_artifacts(run_id, "", dst_path = local_dir)
        artifact_paths = [os.path.join(path, file) for path, currentDirectory, files in os.walk(local_path) for file in files]
        artifacts_binary = {}
        for path in artifact_paths:
            with open(path, mode='rb') as file: # b -> binary
                content = file.read()
                artifacts_binary[path] = content
        return artifacts_binary
    else:
        return {}

# COMMAND ----------

runs_df = spark.createDataFrame(mlflow.search_runs([experiment_id], order_by=["metrics.m DESC"]))

display(runs_df)

# COMMAND ----------

from pyspark.sql.functions import concat, col, lit, create_map

models = (runs_df
          .withColumn("model_path", concat(lit("runs:/"), col("run_id"), lit("/model")))
          .withColumn("metadata", create_map(lit('reader_class'), lit('mlflow'), lit('file_format'), lit('mlflow')))
          .withColumn("model_payload", pickled_model("model_path"))
          .withColumn("artifact_payload", pickled_artifacts("run_id"))          
         )

display(models)

# COMMAND ----------

models.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SHARE ml_sharing COMMENT "Sharing ML models using Delta Sharing"

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER SHARE ml_sharing ADD TABLE vuongnguyen.default.delta_sharing_ml

# COMMAND ----------

# MAGIC %python
# MAGIC import urllib.request
# MAGIC sql("""DROP RECIPIENT IF EXISTS ml_sharing_recipient""")
# MAGIC df = sql("""CREATE RECIPIENT ml_sharing_recipient""")
# MAGIC link = df.collect()[0][4].replace('delta_sharing/retrieve_config.html?','api/2.0/unity-catalog/public/data_sharing_activation/')
# MAGIC urllib.request.urlretrieve(link, "/tmp/ml_sharing_recipient.share")
# MAGIC dbutils.fs.mv("file:/tmp/ml_sharing_recipient.share", "dbfs:/FileStore/ml_sharing_recipient.share")

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT SELECT ON SHARE ml_sharing TO RECIPIENT ml_sharing_recipient

# COMMAND ----------

profile_file = '/FileStore/ml_sharing_recipient.share'
table_url = f"{profile_file}#ml_sharing.default.delta_sharing_ml"

shared_models = spark.read.format('deltaSharing').load(table_url)

display(shared_models)

# COMMAND ----------

from sklearn import datasets
import numpy as np
import pandas as pd

# Load diabetes dataset
diabetes = datasets.load_diabetes()

# Load diabetes dataset
diabetes = datasets.load_diabetes()
X = diabetes.data
cols = ['age', 'sex', 'bmi', 'bp', 's1', 's2', 's3', 's4', 's5', 's6']
data = pd.DataFrame(X, columns=cols)

my_model = pickle.loads(shared_models.collect()[0][1])
my_model.predict(data)
