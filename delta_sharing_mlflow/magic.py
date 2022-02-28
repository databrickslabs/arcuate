from IPython.core.magic import Magics, magics_class, cell_magic
from delta_sharing_mlflow.provider import export_models, export_experiments
from delta_sharing_mlflow.recipient import import_experiments, import_models
from delta_sharing_mlflow.parser import arcuate_parse
import delta_sharing


@magics_class
class ArcuateMagic(Magics):
    @cell_magic
    def arcuate_import_experiment(self, *args):
        """Import ML runs into an experiment"""

        inputs = " ".join(list(args)).replace("\n", " ").replace('"', "")
        ids = arcuate_parse(inputs)

        (experiment_name, table_name) = (ids[0], ids[1])

        if "AS PANDAS" in inputs.upper():
            df = delta_sharing.load_as_pandas(table_name)
        elif "AS SPARK" in inputs.upper():
            df = globals()[table_name]
        else:
            raise NotImplementedError(
                "Syntax not supported. Use AS PANDAS or AS SPARK."
            )

        import_experiments(df, experiment_name)

    @cell_magic
    def arcuate_export_experiment(self, *args):
        """Export ML runs from an experiment into a Delta Sharing table"""

        inputs = " ".join(list(args)).replace("\n", " ").replace('"', "")
        ids = arcuate_parse(inputs)

        (share_name, table_name, experiment_name) = (ids[0], ids[1], ids[2])
        export_experiments(experiment_name, table_name, share_name)

    @cell_magic
    def arcuate_import_model(self, *args):
        """Import ML models from Delta Sharing table into MLFlow registry"""

        inputs = " ".join(list(args)).replace("\n", " ").replace('"', "")
        ids = arcuate_parse(inputs)

        (model_name, table_name) = (ids[0], ids[1])

        if "AS PANDAS" in inputs.upper():
            df = delta_sharing.load_as_pandas(table_name)
        elif "AS SPARK" in inputs.upper():
            df = globals()[table_name]
        else:
            raise NotImplementedError(
                "Syntax not supported. Use AS PANDAS or AS SPARK."
            )

        import_models(df, model_name)

    @cell_magic
    def arcuate_export_model(self, *args):
        """Export ML models into a Delta Sharing table"""

        inputs = " ".join(list(args)).replace("\n", " ").replace('"', "")
        ids = arcuate_parse(inputs)

        (share_name, table_name, model_name) = (ids[0], ids[1], ids[3])
        export_models(model_name, table_name, share_name)
