from IPython.core.magic import Magics, magics_class, cell_magic
from arcuate.provider import export_models, export_experiments
from arcuate.recipient import (
    import_experiments,
    import_models,
    delete_mlflow_model,
    delete_mlflow_experiment,
)
from arcuate.parser import arcuate_parse
import delta_sharing
from mlflow.tracking import MlflowClient
from pyspark.sql import SparkSession


@magics_class
class ArcuateMagic(Magics):

    # initialise spark session & mlflow client
    def __init__(self, shell):
        super(ArcuateMagic, self).__init__(shell)
        self.client = MlflowClient()
        self.spark = SparkSession.builder.getOrCreate()

    @cell_magic
    def arcuate_import_experiment(self, *args):
        """Import ML runs into an experiment"""

        inputs = " ".join(list(args)).replace("\n", " ").replace('"', "")
        ids = arcuate_parse(inputs)

        (experiment_name, table_name) = (ids[0], ids[1])

        if "AS PANDAS" in inputs.upper():
            df = delta_sharing.load_as_pandas(table_name)
        # elif "AS SPARK" in inputs.upper():
        # df = globals()[table_name]
        else:
            raise NotImplementedError("Syntax not supported. Use AS PANDAS")

        # delete the existing runs in the experiment if overwrite specified
        if "OR REPLACE" in inputs.upper():
            delete_mlflow_experiment(self.client, experiment_name)

        import_experiments(self.client, df, experiment_name)

    @cell_magic
    def arcuate_export_experiment(self, *args):
        """Export ML runs from an experiment into a Delta Sharing table"""

        inputs = " ".join(list(args)).replace("\n", " ").replace('"', "")
        ids = arcuate_parse(inputs)

        (share_name, table_name, experiment_name) = (ids[0], ids[1], ids[2])
        export_experiments(self.client, self.spark, experiment_name, table_name, share_name)

    @cell_magic
    def arcuate_import_model(self, *args):
        """Import ML models from Delta Sharing table into MLFlow registry"""

        inputs = " ".join(list(args)).replace("\n", " ").replace('"', "")
        ids = arcuate_parse(inputs)

        (model_name, table_name) = (ids[1], ids[2])

        if "AS PANDAS" in inputs.upper():
            df = delta_sharing.load_as_pandas(table_name)
        # elif "AS SPARK" in inputs.upper():
        # df = globals()[table_name]
        else:
            raise NotImplementedError("Syntax not supported. Use AS PANDAS")

        # delete the existing model if overwrite specified
        if "OR REPLACE" in inputs.upper():
            delete_mlflow_model(self.client, model_name)

        # import the models
        import_models(self.client, df, model_name)

    @cell_magic
    def arcuate_export_model(self, *args):
        """Export ML models into a Delta Sharing table"""

        inputs = " ".join(list(args)).replace("\n", " ").replace('"', "")
        ids = arcuate_parse(inputs)

        (share_name, table_name, model_name) = (ids[0], ids[1], ids[3])
        export_models(self.client, self.spark, model_name, table_name, share_name)
