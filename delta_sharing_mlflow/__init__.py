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
)
from delta_sharing_mlflow.parser import arcuate_parse
from delta_sharing_mlflow.magic import ArcuateMagic
from delta_sharing_mlflow.version import __version__

__all__ = [
    "pickle_model_udf",
    "pickle_artifacts_udf",
    "normalize_mlflow_df",
    "export_models",
    "load_delta_sharing_ml_model",
    "write_and_log_artifacts",
    "chunks",
    "import_models",
    "arcuate_parse",
    "ArcuateMagic",
    "__version__",
]

ip = get_ipython()
print(
    "Adding Magic to support %python %%arcuate_export_model, %%arcuate_export_experiment, %%arcuate_export_experiment, %%arcuate_import"
)
ip.register_magics(ArcuateMagic)
