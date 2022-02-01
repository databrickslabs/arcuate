from IPython.core.magic import Magics, magics_class, cell_magic
from delta_sharing_mlflow.provider import export_models
from delta_sharing_mlflow.recipient import import_models
from delta_sharing_mlflow.parser import arcuate_parse

@magics_class
class ArcuateMagic(Magics):

    @cell_magic
    def arcuate_import(self, *args):
        "Import ML models into an experiment"

        inputs = " ".join(list(args)).replace("\n", " ").replace('"', "")
        ids = arcuate_parse(inputs)

        (experiment_name, table_name) = (ids[0], ids[1])

        if "AS PANDAS" in inputs.upper():
            df = globals()[table_name].toPandas()
        elif "AS SPARK" in inputs.upper():
            df = globals()[table_name]
        else:
            raise NotImplementedError(
                "Syntax not supported. Use AS PANDAS or AS SPARK."
            )

        import_models(df, experiment_name)

    @cell_magic
    def arcuate_export(self, *args):
        "Export ML models into a Delta Sharing table"

        inputs = " ".join(list(args)).replace("\n", " ").replace('"', "")
        ids = arcuate_parse(inputs)

        (share_name, table_name, experiment_name) = (ids[0], ids[1], ids[2])

        export_models(experiment_name, table_name, share_name)        
