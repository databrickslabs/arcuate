import shutil, os, random, string
import cloudpickle
import mlflow
import pandas as pd
from mlflow.tracking import MlflowClient
from pyspark.sql import functions as F
from pyspark.sql.types import BinaryType, MapType, StringType
from pyspark.sql import DataFrame

@F.pandas_udf(BinaryType())
def pickle_model_udf(model_paths: pd.Series) -> pd.Series:
    def pickle_model(model_path: str) -> bytes:
        return cloudpickle.dumps(mlflow.pyfunc.load_model(f"runs:/{model_path}/model"))
    return model_paths.apply(pickle_model)

@F.pandas_udf(MapType(StringType(), BinaryType()))
def pickle_artifacts_udf(run_ids: pd.Series)-> pd.Series:
    def pickle_artifacts(run_id: str):
        client = MlflowClient()
        artifacts = client.list_artifacts(run_id)
        artifacts_binary = {}
        
        # ignore the ML model
        ignored_paths = ["model/MLModel", "model/model.pkl", "model/requirements.txt", "model/conda.yaml"]

        if len(artifacts) > 0: # Because of https://github.com/mlflow/mlflow/issues/2839
            local_dir = "/tmp/artifact_downloads" + ''.join(random.choices(string.ascii_lowercase + string.digits, k=10))
            if os.path.exists(local_dir):
                shutil.rmtree(local_dir)
            os.mkdir(local_dir)
            local_path = client.download_artifacts(run_id, "", dst_path = local_dir)
            artifact_paths = [os.path.join(path, file) for path, _, files in os.walk(local_path) for file in files]
            for path in artifact_paths:
                if not any(ignored in path for ignored in ignored_paths):
                    with open(path, mode='rb') as file: # b -> binary
                        content = file.read()
                        relative_path = path.replace(local_dir, '')
                        artifacts_binary[relative_path] = content
            shutil.rmtree(local_dir)
        return artifacts_binary
    return run_ids.apply(pickle_artifacts)

def normalize_mlflow_df(experiment_infos_df: DataFrame) -> DataFrame:
    # now ignore a few columns
    ignored_cols = ["mlflow.user", "databricks.notebookID", "artifact_uri"]
    columns = [cn for cn in experiment_infos_df.columns if not any(ignored in cn for ignored in ignored_cols)]
    metrics_subschema = [cn for cn in columns if "metrics." in cn]
    params_subschema  = [cn for cn in columns if "params." in cn]
    tags_subschema    = [cn for cn in columns if "tags." in cn and "mlflow.user" not in cn and "databricks.notebookID" not in cn]
    run_info_subschema = [cn for cn in columns if cn not in tags_subschema + params_subschema + metrics_subschema]
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