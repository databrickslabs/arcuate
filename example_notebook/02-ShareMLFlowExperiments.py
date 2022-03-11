# Databricks notebook source
import urllib.request

sql("""DROP RECIPIENT IF EXISTS ml_sharing_recipient""")
df = sql("""CREATE RECIPIENT ml_sharing_recipient""")
link = df.collect()[0][6].replace(
    "delta_sharing/retrieve_config.html?",
    "api/2.0/unity-catalog/public/data_sharing_activation/",
)
urllib.request.urlretrieve(link, "/tmp/ml_sharing_recipient.share")
dbutils.fs.mv(
    "file:/tmp/ml_sharing_recipient.share", "dbfs:/FileStore/ml_sharing_recipient.share"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT SELECT ON SHARE ml_sharing to RECIPIENT ml_sharing_recipient
