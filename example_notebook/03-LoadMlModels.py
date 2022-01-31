# Databricks notebook source
# MAGIC %pip install delta-sharing

# COMMAND ----------

# MAGIC %pip install -r ./../requirements.txt

# COMMAND ----------

import delta_sharing

# COMMAND ----------

# MAGIC %fs head /FileStore/milos_colic/arcuate.share

# COMMAND ----------

profile_file = '/dbfs/FileStore/milos_colic/arcuate.share'

# Create a SharingClient
client = delta_sharing.SharingClient(profile_file)

# List all shared tables.
client.list_all_tables()

# COMMAND ----------

profile_file = '/dbfs/FileStore/milos_colic/arcuate.share'
table_url = f"{profile_file}#ml_sharing.default.delta_sharing_ml"

# Use delta sharing client to load data
flights_df = delta_sharing.load_as_pandas(table_url)

flights_df.head(10)

# COMMAND ----------

flights_df = delta_sharing.load_as_spark(table_url.replace("/dbfs/", "dbfs:/"))

# COMMAND ----------

# MAGIC %fs ls Users/milos_colic

# COMMAND ----------

# MAGIC %fs mkdirs Users/milos_colic/arcuate

# COMMAND ----------

flights_df.write.format("delta").save("dbfs:/Users/milos_colic/arcuate/ml_share")

# COMMAND ----------

flights_df = spark.read.format("delta").load("dbfs:/Users/milos_colic/arcuate/ml_share")

# COMMAND ----------

flights_df.count()

# COMMAND ----------

display(flights_df)

# COMMAND ----------

flights_df.printSchema()

# COMMAND ----------

flight_pd = flights_df.toPandas()
flight_pd = flight_pd.reset_index()  # make sure indexes pair with number of rows

# COMMAND ----------

type(flight_pd.iloc[0]["model_payload"])

# COMMAND ----------

from itertools import islice

def chunks(data, SIZE=10000):
  it = iter(data)
  for i in range(0, len(data), SIZE):
    yield {k:data[k] for k in islice(it, SIZE)}

# COMMAND ----------

json.loads("[{\"artifact_path\":\"model\",\"saved_input_example_info\":{\"artifact_path\":\"input_example.json\",\"type\":\"dataframe\",\"pandas_orient\":\"split\"},\"signature\":{\"inputs\":\"[{\\\"name\\\": \\\"age\\\", \\\"type\\\": \\\"double\\\"}, {\\\"name\\\": \\\"workclass\\\", \\\"type\\\": \\\"string\\\"}, {\\\"name\\\": \\\"fnlwgt\\\", \\\"type\\\": \\\"double\\\"}, {\\\"name\\\": \\\"education\\\", \\\"type\\\": \\\"string\\\"}, {\\\"name\\\": \\\"education_num\\\", \\\"type\\\": \\\"double\\\"}, {\\\"name\\\": \\\"marital_status\\\", \\\"type\\\": \\\"string\\\"}, {\\\"name\\\": \\\"occupation\\\", \\\"type\\\": \\\"string\\\"}, {\\\"name\\\": \\\"relationship\\\", \\\"type\\\": \\\"string\\\"}, {\\\"name\\\": \\\"race\\\", \\\"type\\\": \\\"string\\\"}, {\\\"name\\\": \\\"sex\\\", \\\"type\\\": \\\"string\\\"}, {\\\"name\\\": \\\"capital_gain\\\", \\\"type\\\": \\\"double\\\"}, {\\\"name\\\": \\\"capital_loss\\\", \\\"type\\\": \\\"double\\\"}, {\\\"name\\\": \\\"hours_per_week\\\", \\\"type\\\": \\\"double\\\"}, {\\\"name\\\": \\\"native_country\\\", \\\"type\\\": \\\"string\\\"}]\",\"outputs\":\"[{\\\"type\\\": \\\"tensor\\\", \\\"tensor-spec\\\": {\\\"dtype\\\": \\\"object\\\", \\\"shape\\\": [-1]}}]\"},\"flavors\":{\"python_function\":{\"model_path\":\"model.pkl\",\"loader_module\":\"mlflow.sklearn\",\"python_version\":\"3.8.10\",\"env\":\"conda.yaml\"},\"sklearn\":{\"pickled_model\":\"model.pkl\",\"sklearn_version\":\"0.24.1\",\"serialization_format\":\"cloudpickle\"}},\"run_id\":\"a0fe4422846f4a62ad68136bc53aa5c2\",\"utc_time_created\":\"2022-01-25 16:07:14.954198\"}]")[0]["artifact_path"]

# COMMAND ----------

import mlflow
import cloudpickle
import json
import sys

flight_pd = flights_df.toPandas()
flight_pd = flight_pd.reset_index()  # make sure indexes pair with number of rows

for index, row in flight_pd.iterrows():
  with mlflow.start_run():
    tags = row["tags"]
    tags.pop('mlflow.user', None)
    model_loader = json.loads(tags['mlflow.log-model.history'])[0]["flavors"]["python_function"]["loader_module"]
    artifact_path = json.loads(tags['mlflow.log-model.history'])[0]["artifact_path"]
    tags.pop('mlflow.log-model.history', None)
    mlflow.set_tags(tags)
    mlflow.log_metrics(row["metrics"])
    params = row["params"]
    for chunk in chunks(params, 90):
      mlflow.log_params(chunk)
    model_payload = row["model_payload"]
    model = cloudpickle.loads(model_payload)
    sys.modules[model_loader].log_model(model, artifact_path)
    #mlflow.log_artifacts(row["artifact_payload"])

# COMMAND ----------


