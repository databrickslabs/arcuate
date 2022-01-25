import shutil, os
import dill
import mlflow
import pandas as pd
from mlflow.tracking import MlflowClient
from pyspark.sql.functions import udf, pandas_udf
from pyspark.sql.types import BinaryType, MapType, StringType

@pandas_udf(BinaryType())
def pickle_model_udf(model_paths: pd.Series) -> pd.Series:
    def pickle_model(model_path: str) -> bytes:
        return dill.dumps(mlflow.pyfunc.load_model(f"runs:/{model_path}/model"))
    return model_paths.apply(pickle_model)

@pandas_udf(MapType(StringType(), BinaryType()))
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