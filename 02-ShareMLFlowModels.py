# Databricks notebook source
# MAGIC %pip install delta-sharing

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SHARE IF NOT EXISTS ml_sharing COMMENT "Sharing ML models using Delta Sharing"

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER SHARE ml_sharing ADD TABLE vuongnguyen.default.delta_sharing_ml

# COMMAND ----------

import urllib.request
sql("""DROP RECIPIENT IF EXISTS ml_sharing_recipient""")
df = sql("""CREATE RECIPIENT ml_sharing_recipient""")
link = df.collect()[0][4].replace('delta_sharing/retrieve_config.html?','api/2.0/unity-catalog/public/data_sharing_activation/')
urllib.request.urlretrieve(link, "/tmp/ml_sharing_recipient.share")
dbutils.fs.mv("file:/tmp/ml_sharing_recipient.share", "dbfs:/FileStore/ml_sharing_recipient.share")

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT SELECT ON SHARE ml_sharing TO RECIPIENT ml_sharing_recipient

# COMMAND ----------

from mlpickling import load_delta_sharing_ml_model

# COMMAND ----------

profile_file = '/FileStore/ml_sharing_recipient.share'
table_url = f"{profile_file}#ml_sharing.default.delta_sharing_ml"

my_model = load_delta_sharing_ml_model(table_url)
input_df = spark.read.table("main.default.adult").toPandas()
my_model.predict(input_df)
