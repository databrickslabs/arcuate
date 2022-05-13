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
    def arcuate(self, *args):
        """Import ML runs into an experiment"""

        inputs = " ".join(list(args)).replace("\n", " ").replace('"', "")
        ids = arcuate_parse(inputs)

        # provider actions
        if ids[1].upper() == "SHARE":

            (share_name, table_name, object_name) = (ids[2], ids[3], ids[5])

            # export model
            if ids[4].upper() == "MODE":
                model_name = object_name
                export_models(self.client, self.spark, model_name, table_name, share_name)
            # export experiment
            elif ids[4].upper() == "EXPLAIN":
                experiment_name = object_name
                export_experiments(self.client, self.spark, experiment_name, table_name, share_name)

        # recipient actions
        else:

            (action, object_name, table_name) = (ids[0], ids[2], ids[3])

            if "AS PANDAS" in inputs.upper():
                df = delta_sharing.load_as_pandas(table_name)
            # elif "AS SPARK" in inputs.upper():
            # TO BE IMPLEMENTED
            else:
                raise NotImplementedError("Syntax not supported. Use AS PANDAS")

            # import model
            if ids[1] == "MODE":
                model_name = object_name

                # delete the existing model if overwrite specified
                if action.upper() == "CREATE OR REPLACE":
                    delete_mlflow_model(self.client, model_name)

                # import the models
                import_models(self.client, df, model_name)

            # import experiment
            elif ids[1] == "EXPLAIN":

                experiment_name = object_name

                # delete the existing runs in the experiment if overwrite specified
                if action.upper() == "CREATE OR REPLACE":
                    delete_mlflow_experiment(self.client, experiment_name)

                # import the experiments
                import_experiments(self.client, df, experiment_name)
