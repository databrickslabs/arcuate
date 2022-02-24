# Databricks notebook source
# MAGIC %pip install -r ./../requirements.txt

# COMMAND ----------

import delta_sharing
from delta_sharing_mlflow import ArcuateMagic

# COMMAND ----------

profile_file = "/dbfs/FileStore/ml_sharing_recipient.share"
# Create a SharingClient
client = delta_sharing.SharingClient(profile_file)

# List all shared tables.
client.list_all_tables()

# COMMAND ----------

table_url = f"{profile_file}#ml_sharing.default.delta_sharing_ml"

# Use delta sharing client to load data
# ml_pd = delta_sharing.load_as_pandas(table_url)

# COMMAND ----------

# MAGIC %%arcuate_import
# MAGIC create/merge/append experiment '/Users/vuong.nguyen+uc@databricks.com/my_test_experiment' as pandas '/dbfs/FileStore/ml_sharing_recipient.share#ml_sharing.default.delta_sharing_ml'
