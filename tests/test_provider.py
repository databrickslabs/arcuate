from pytest_mock import MockerFixture
from arcuate.provider import *
from pyspark.sql import SparkSession
from mlflow.entities.model_registry import ModelVersion


def mlflow_setup(mocker: MockerFixture):
    mock_mlflow_client = mocker.MagicMock()
    mock_mlflow_client.configure_mock(
        **{
            "search_model_versions.return_value": [ModelVersion("name", 1, 1647016654551)],
            "get_experiment_by_name.experiment_id.return_value": "name",
        }
    )
    return mock_mlflow_client


def test_normalize_experiment_df():
    spark = SparkSession.builder.getOrCreate()

    df = spark.read.parquet("tests/test_provider/experiments.parquet")

    normalised_df = spark.read.parquet("tests/test_provider/norm.parquet")

    df = normalize_experiment_df(df)

    assert df.columns == normalised_df.columns


def test_export_experiments(mocker: MockerFixture):
    mock_mlflow_client = mlflow_setup(mocker)
    mock_spark = mocker.patch("pyspark.sql.SparkSession")
    mocker.patch("arcuate.provider.pickle_df")
    mocker.patch("arcuate.provider.normalize_experiment_df")

    mocked_search = mocker.patch("mlflow.search_runs")
    mocked_search.return_value = pd.read_parquet("tests/test_provider/experiments.parquet")

    export_experiments(mock_mlflow_client, mock_spark, "experiment", "table", "share")

    assert [
        mocker.call("CREATE SHARE IF NOT EXISTS share"),
        mocker.call("ALTER SHARE share ADD TABLE table"),
    ] == mock_spark.sql.mock_calls


def test_export_models(mocker: MockerFixture):
    mock_mlflow_client = mlflow_setup(mocker)
    mock_spark = mocker.patch("pyspark.sql.SparkSession")

    mocker.patch("arcuate.provider.pickle_model")

    export_models(mock_mlflow_client, mock_spark, "model", "table", "share")

    assert [
        mocker.call("CREATE SHARE IF NOT EXISTS share"),
        mocker.call("ALTER SHARE share ADD TABLE table"),
    ] == mock_spark.sql.mock_calls
