from delta_sharing_mlflow.provider import (
    pickle_run_model_udf,
    pickle_model_udf,
    pickle_artifacts_udf,
    normalize_experiment_df,
    export_models,
    export_experiments,
)
from delta_sharing_mlflow.recipient import (
    write_and_log_artifacts,
    chunks,
    import_experiments,
    import_experiment,
    import_models,
    delete_mlflow_model,
    delete_mlflow_experiment,
)
from delta_sharing_mlflow.parser import arcuate_parse
from delta_sharing_mlflow.version import __version__

__all__ = [
    "pickle_run_model_udf",
    "pickle_model_udf",
    "pickle_artifacts_udf",
    "normalize_experiment_df",
    "normalize_mlflow_df",
    "export_models",
    "export_experiments",
    "load_delta_sharing_ml_model",
    "write_and_log_artifacts",
    "chunks",
    "import_experiments",
    "import_experiment",
    "import_models",
    "delete_mlflow_model",
    "delete_mlflow_experiment",
    "arcuate_parse",
    "ArcuateMagic",
    "__version__",
]

## only import IPython magic if IPython exists
try:
    from delta_sharing_mlflow.magic import ArcuateMagic

    ip = get_ipython()
    print(
        "Adding Magic to support %python %%arcuate_export_model, %%arcuate_export_experiment, %%arcuate_import_model, %%arcuate_import_experiment"
    )
    ip.register_magics(ArcuateMagic)
except Exception:
    pass
