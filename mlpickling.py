import shutil, os
import cloudpickle
import mlflow
import pandas as pd
from mlflow.tracking import MlflowClient
from pyspark.sql import functions as F
from pyspark.sql.types import BinaryType, MapType, StringType
from pyspark.sql import DataFrame
import delta_sharing

@F.pandas_udf(BinaryType())
def pickle_model_udf(model_paths: pd.Series) -> pd.Series:
    def pickle_model(model_path: str) -> bytes:
        return cloudpickle.dumps(mlflow.pyfunc.load_model(f"runs:/{model_path}/model"))
    return model_paths.apply(pickle_model)

@F.pandas_udf(MapType(StringType(), BinaryType()))
def pickle_artifacts_udf(run_ids: pd.Series)-> pd.Series:
    client = MlflowClient()
    def pickle_artifacts(run_id: str):
        client = MlflowClient()
        artifacts = client.list_artifacts(run_id)
        artifacts_binary = {}

        if len(artifacts) > 0: # Because of https://github.com/mlflow/mlflow/issues/2839
            local_dir = "/tmp/artifact_downloads"
            if os.path.exists(local_dir):
                shutil.rmtree(local_dir)
            os.mkdir(local_dir)
            local_path = client.download_artifacts(run_id, "", dst_path = local_dir)
            artifact_paths = [os.path.join(path, file) for path, currentDirectory, files in os.walk(local_path) for file in files]
            for path in artifact_paths:
                with open(path, mode='rb') as file: # b -> binary
                    content = file.read()
                    artifacts_binary[path] = content
        return artifacts_binary
    return run_ids.apply(pickle_artifacts)

def normalize_mlflow_df(experiment_infos_df: DataFrame) -> DataFrame:
    metrics_subschema = [cn for cn in experiment_infos_df.columns if "metrics." in cn]
    params_subschema  = [cn for cn in experiment_infos_df.columns if "params." in cn]
    tags_subschema    = [cn for cn in experiment_infos_df.columns if "tags." in cn]
    run_info_subschema = [cn for cn in experiment_infos_df.columns if cn not in tags_subschema + params_subschema + metrics_subschema]
    return (experiment_infos_df
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

def load_delta_sharing_ml_model(table_url: str):
    shared_models = delta_sharing.load_as_spark(table_url)
    return cloudpickle.loads(shared_models.collect()[0]["model_payload"])