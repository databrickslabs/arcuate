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

import mlflow
import os
import shutil

def write_and_log_artifacts(artifacts):
  os.mkdir("/tmp_artifacts")
  for key, value in artifacts.items():
    f = open(f"/tmp_artifacts/{key.split('/')[-1]}", 'wb')
    f.write(value)
    f.close()
  mlflow.log_artifacts("/tmp_artifacts")  
  shutil.rmtree("/tmp_artifacts")

# COMMAND ----------

import mlflow
import cloudpickle
import json
import sys
from mlflow.models.signature import ModelSignature

flight_pd = flights_df.toPandas()
flight_pd = flight_pd.reset_index()  # make sure indexes pair with number of rows

experiment_id = mlflow.create_experiment("/Users/milos.colic@databricks.com/Arcuate")

for index, row in flight_pd.iterrows():
  with mlflow.start_run(experiment_id=experiment_id):
    tags = row["tags"]
    tags.pop('mlflow.user', None)
    mlflow_log_model_history = json.loads(tags['mlflow.log-model.history'])[-1] #I dont think we know which is the latest
    model_loader = mlflow_log_model_history["flavors"]["python_function"]["loader_module"]
    artifact_path = mlflow_log_model_history["artifact_path"]
    signature = ModelSignature.from_dict(mlflow_log_model_history["signature"])
    tags.pop('mlflow.log-model.history', None)
    mlflow.set_tags(tags)
    mlflow.log_metrics(row["metrics"])
    params = row["params"]
    for chunk in chunks(params, 90):
      mlflow.log_params(chunk)
    model_payload = row["model_payload"]
    model = cloudpickle.loads(model_payload)
    sys.modules[model_loader].log_model(model, artifact_path, signature=signature)
    write_and_log_artifacts(row["artifact_payload"])

# COMMAND ----------

from IPython.core.magic import Magics, magics_class, cell_magic
import mlflow
import cloudpickle
import json
import sys
from mlflow.models.signature import ModelSignature
from itertools import islice
import mlflow
import os
import shutil
import sqlparse
from sqlparse.tokens import Whitespace
from typing import List
import re


@magics_class
class ArcuateMagic(Magics):

  def arcuate_parse(self, in_query:str) -> List[str]:
      query = in_query.upper().replace(' EXPERIMENT ', ' MODE ').replace(' PANDAS ', ' SELECT ').replace(' SPARK ', ' SELECT ')
      tokens = [item.value for item in sqlparse.parse(query)[0] if item.ttype != Whitespace]
      if tokens[0]!= 'CREATE' or tokens[1] not in ['SHARE', 'MODE'] or tokens[3] != 'AS' or tokens[4] != 'SELECT':
          raise NotImplementedError("syntax not supported")

      pattern = re.compile(" experiment ", re.IGNORECASE)
      query = pattern.sub(" ", in_query)
      pattern = re.compile(" pandas ", re.IGNORECASE)
      query = pattern.sub(" select ", query)
      pattern = re.compile(" spark ", re.IGNORECASE)
      query = pattern.sub(" select ", query)

      tokens = sqlparse.parse(query)[0].tokens
      ids = [item.value for item in tokens if (str(item.ttype) == "Token.Literal.String.Single" or str(item.ttype) == "None") and item.value.upper() != 'AS']

      return ids
  
  def write_and_log_artifacts(artifacts):
    os.mkdir("/tmp_artifacts")
    for key, value in artifacts.items():
      f = open(f"/tmp_artifacts/{key.split('/')[-1]}", 'wb')
      f.write(value)
      f.close()
    mlflow.log_artifacts("/tmp_artifacts")  
    shutil.rmtree("/tmp_artifacts")
  
  def chunks(data, SIZE=10000):
    it = iter(data)
    for i in range(0, len(data), SIZE):
      yield {k:data[k] for k in islice(it, SIZE)}
  
  def import_models(self, df, experiment_name):
    experiment_id = mlflow.create_experiment(experiment_name)
    for index, row in df.iterrows():
      with mlflow.start_run(experiment_id=experiment_id):
        tags = row["tags"]
        tags.pop('mlflow.user', None)
        mlflow_log_model_history = json.loads(tags['mlflow.log-model.history'])[-1] #I dont think we know which is the latest
        model_loader = mlflow_log_model_history["flavors"]["python_function"]["loader_module"]
        artifact_path = mlflow_log_model_history["artifact_path"]
        signature = ModelSignature.from_dict(mlflow_log_model_history["signature"])
        tags.pop('mlflow.log-model.history', None)
        mlflow.set_tags(tags)
        metrics = row["metrics"]
        for chunk in chunks(metrics, 90):
          mlflow.log_metrics(chunk)
        params = row["params"]
        for chunk in chunks(params, 90):
          mlflow.log_params(chunk)
        model_payload = row["model_payload"]
        model = cloudpickle.loads(model_payload)
        sys.modules[model_loader].log_model(model, artifact_path, signature=signature)
        write_and_log_artifacts(row["artifact_payload"])

  @cell_magic
  def arcuate_import(self, *args):
    "Import ML models into an experiment"
        
    inputs = " ".join(list(args)).replace("\n", " ").replace("\"", "")
    ids = self.arcuate_parse(inputs)
        
    experiment_name = ids[0].replace("'", "")
    table_name = ids[1].replace("'", "")
    
    if "AS PANDAS" in inputs.upper():
      df = globals()[table_name]
    elif "AS SPARK" in inpits.upper():
      df = globals()[table_name].toPandas()
    else:
      raise NotImplementedError("Syntax not supported. Use AS PANDAS or AS SPARK.")
    
    self.import_models(df, experiment_name)
    
    

ip = get_ipython()
print("Adding Magic to support %python %%arcuate_import")
ip.register_magics(ArcuateMagic)

# COMMAND ----------

# MAGIC %%arcuate_import 
# MAGIC create experiment '/Users/milos.colic@databricks.com/my_test_experiment' as pandas 'flight_pd'

# COMMAND ----------


