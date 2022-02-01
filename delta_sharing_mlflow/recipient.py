import mlflow
import cloudpickle
import json
import sys
from mlflow.models.signature import ModelSignature
from itertools import islice
import mlflow
import os
import shutil


def write_and_log_artifacts(artifacts):
    os.mkdir("/tmp_artifacts")
    for key, value in artifacts.items():
        f = open(f"/tmp_artifacts/{key.split('/')[-1]}", "wb")
        f.write(value)
        f.close()
    mlflow.log_artifacts("/tmp_artifacts")
    shutil.rmtree("/tmp_artifacts")


def chunks(data, SIZE=10000):
    it = iter(data)
    for _ in range(0, len(data), SIZE):
        yield {k: data[k] for k in islice(it, SIZE)}


def import_models(df, experiment_name):
    experiment_id = mlflow.create_experiment(experiment_name)
    for _, row in df.iterrows():
        with mlflow.start_run(experiment_id=experiment_id):
            tags = row["tags"]
            mlflow_log_model_history = json.loads(tags["mlflow.log-model.history"])[
                -1
            ]  # I dont think we know which is the latest
            model_loader = mlflow_log_model_history["flavors"]["python_function"][
                "loader_module"
            ]
            artifact_path = mlflow_log_model_history["artifact_path"]
            signature = ModelSignature.from_dict(mlflow_log_model_history["signature"])
            tags.pop("mlflow.log-model.history", None)
            mlflow.set_tags(tags)
            metrics = row["metrics"]
            for chunk in chunks(metrics, 90):
                mlflow.log_metrics(chunk)
            params = row["params"]
            for chunk in chunks(params, 90):
                mlflow.log_params(chunk)
            model_payload = row["model_payload"]
            model = cloudpickle.loads(model_payload)
            sys.modules[model_loader].log_model(
                model, artifact_path, signature=signature
            )
            write_and_log_artifacts(row["artifact_payload"])
