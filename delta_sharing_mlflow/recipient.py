import mlflow
import cloudpickle
import json
import sys
from mlflow.models.signature import ModelSignature
from mlflow.tracking import MlflowClient
from itertools import islice
import mlflow
import os
import shutil
import random
import string
import multiprocessing as mp
import numpy as np
import pandas as pd


def write_and_log_artifacts(artifacts: dict) -> None:
    random_suffix = "".join(
        random.choices(string.ascii_lowercase + string.digits, k=10)
    )
    local_dir = f"/tmp/{random_suffix}"
    if os.path.exists(local_dir):
        shutil.rmtree(local_dir)
    os.mkdir(local_dir)
    os.mkdir(f"{local_dir}/model")
    for path, content in artifacts.items():
        f = open(f"{local_dir}/{path}", "wb")
        f.write(content)
        f.close()
    mlflow.log_artifacts(local_dir)
    shutil.rmtree(local_dir)


def chunks(data: dict, SIZE: int = 10000) -> dict:
    it = iter(data)
    for _ in range(0, len(data), SIZE):
        yield {k: data[k] for k in islice(it, SIZE)}


def import_experiment(df: pd.DataFrame, experiment_id: str) -> None:
    for _, row in df.iterrows():
        with mlflow.start_run(experiment_id=experiment_id):
            tags = dict(row["tags"])
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
            metrics = dict(row["metrics"])
            for chunk in chunks(metrics, 90):
                mlflow.log_metrics(chunk)
            params = dict(row["params"])
            for chunk in chunks(params, 90):
                mlflow.log_params(chunk)
            model_payload = row["model_payload"]
            model = cloudpickle.loads(model_payload)
            sys.modules[model_loader].log_model(
                model, artifact_path, signature=signature
            )
            write_and_log_artifacts(dict(row["artifact_payload"]))


def import_experiments(df: pd.DataFrame, experiment_name: str) -> None:
    try:
        experiment_id = mlflow.create_experiment(experiment_name)
    except:
        client = MlflowClient()
        experiment_id = client.get_experiment_by_name(experiment_name).experiment_id

    parallel = mp.cpu_count()
    dfs = np.array_split(df, parallel)

    with mp.Pool(parallel) as p:
        for d in dfs:
            p.apply_async(import_experiments, args=(d, experiment_id))
        p.close()
        p.join()


def import_models(df: pd.DataFrame, model_name: str) -> None:

    client = MlflowClient()
    for _, row in df.iterrows():
        model_payload = row["model_payload"]
        current_stage = row["current_stage"]
        description = row["description"]

        # load the model and extract the correct flavour
        model = cloudpickle.loads(model_payload)
        model_loader = model.metadata.flavors["python_function"]["loader_module"]

        # load the model to registry using the correct flavour
        sys.modules[model_loader].log_model(
            model, artifact_path="model", registered_model_name=model_name
        )
        # get the latest version
        versions = [
            int(dict(mv)["version"])
            for mv in client.search_model_versions(f"name='{model_name}'")
        ]
        max_version = max(versions)

        # transition to the appropriate stage if needed
        if current_stage != "None":
            client.transition_model_version_stage(
                name=model_name, version=max_version, stage=current_stage
            )

        # add description if needed
        if description != "":
            client.update_model_version(
                name=model_name, version=max_version, description=description
            )

def delete_mlflow_model(model_name: str) -> None:
    client = MlflowClient()
    model_versions = client.search_model_versions(f"name='{model_name}'")
    
    # delete existing version first
    for mv in model_versions:
        version = dict(mv)['version']
        client.transition_model_version_stage(name=model_name, version=version, stage="Archived")
        client.delete_model_version(name=model_name, version=version)

    # Delete a registered model along with all its versions
    try:
        client.delete_registered_model(name=model_name)
    except Exception:
        pass    