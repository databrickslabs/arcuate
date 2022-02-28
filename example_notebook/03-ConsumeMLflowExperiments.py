# Databricks notebook source
# MAGIC %pip install -r ./../requirements.txt

# COMMAND ----------

import delta_sharing
import delta_sharing_mlflow

# COMMAND ----------

profile_file = "/dbfs/FileStore/ml_sharing_recipient.share"
# Create a SharingClient
client = delta_sharing.SharingClient(profile_file)

# List all shared tables.
client.list_all_tables()

# COMMAND ----------

# MAGIC %%arcuate_import_experiment
# MAGIC create experiment '/Users/vuong.nguyen+uc@databricks.com/my_test_experiment' as pandas '/dbfs/FileStore/ml_sharing_recipient.share#ml_sharing.default.delta_sharing_ml_experiment'

# COMMAND ----------

# MAGIC %%arcuate_import_experiment
# MAGIC create experiment '/Users/vuong.nguyen+uc@databricks.com/my_test_experiment' as pandas '/dbfs/FileStore/ml_sharing_recipient.share#ml_sharing.default.delta_sharing_ml_experiment'

# COMMAND ----------

# MAGIC %%arcuate_import_experiment
# MAGIC create experiment overwrite '/Users/vuong.nguyen+uc@databricks.com/my_test_experiment' as pandas '/dbfs/FileStore/ml_sharing_recipient.share#ml_sharing.default.delta_sharing_ml_experiment'
