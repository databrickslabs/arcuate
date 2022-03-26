from unittest.mock import MagicMock
import pytest
from pytest_mock import MockerFixture
from arcuate.recipient import *


def mlflow_setup(mocker: MockerFixture):
    mock_mlflow_client = mocker.MagicMock()
    mock_mlflow_client.configure_mock(
        **{
            "search_model_versions.return_value": [{"version": 1}, {"version": 2}],
            "get_experiment_by_name.experiment_id.return_value": "name",
        }
    )
    return mock_mlflow_client


@pytest.mark.parametrize(
    "length, len_chunk, num_chunk",
    [(1000, 100, 10), (10, 100, 1), (100000, 1, 100000)],
)
def test_chunk(length: int, len_chunk: int, num_chunk: int):
    param = dict([(i, i) for i in range(length)])
    chunked_param = chunks(param, len_chunk)
    count = 0
    for param in chunked_param:
        count += 1
        assert len(param) <= len_chunk
    assert count == num_chunk


def test_write_and_log_artifacts(mocker: MockerFixture):

    mocked_log_artifacts = mocker.patch("mlflow.log_artifacts")
    mocker.patch("shutil.rmtree")
    mocker.patch("os.mkdir")
    mocked_open = mocker.patch("builtins.open", mocker.mock_open())

    artifacts = {"a": "b", "c": "d"}

    write_and_log_artifacts(artifacts)

    mocked_log_artifacts.assert_called_once()
    assert 2 == mocked_open.call_count


def test_delete_mlflow_experiment(mocker: MockerFixture):

    mock_mlflow_client = mlflow_setup(mocker)

    mock_delete = mocker.patch("mlflow.delete_run")
    mock_search = mocker.patch("mlflow.search_runs")

    mock_search.return_value = pd.DataFrame(data={"run_id": [1, 2]})

    delete_mlflow_experiment(mock_mlflow_client, "name")

    assert [
        mocker.call(1),
        mocker.call(2),
    ] == mock_delete.mock_calls
    mock_mlflow_client.get_experiment_by_name.assert_called_once_with("name")


def test_delete_mlflow_model(mocker: MockerFixture):
    mock_mlflow_client = mlflow_setup(mocker)
    delete_mlflow_model(mock_mlflow_client, "name")

    mock_mlflow_client.search_model_versions.assert_called_once_with("name='name'")

    assert [
        mocker.call(name="name", version=1, stage="Archived"),
        mocker.call(name="name", version=2, stage="Archived"),
    ] == mock_mlflow_client.transition_model_version_stage.mock_calls

    assert [
        mocker.call(name="name", version=1),
        mocker.call(name="name", version=2),
    ] == mock_mlflow_client.delete_model_version.mock_calls

    mock_mlflow_client.delete_registered_model.assert_called_once_with(name="name")

    mock_mlflow_client.delete_registered_model.side_effect = Exception()
    delete_mlflow_model(mock_mlflow_client, "name")


def test_import_models(mocker: MockerFixture):

    mock_mlflow_client = mlflow_setup(mocker)

    mocked_pickle = mocker.patch("cloudpickle.loads")
    mocked_pickle().metadata.flavors.__getitem__().__getitem__.return_value = "mlflow.sklearn"

    mocked_log = mocker.patch("mlflow.sklearn.log_model")

    df: pd.DataFrame = pd.read_csv("tests/test_recipient/models.csv")

    import_models(mock_mlflow_client, df, "name")

    assert 7 == mock_mlflow_client.search_model_versions.call_count
    assert 7 == mocked_log.call_count
    assert 2 == mock_mlflow_client.transition_model_version_stage.call_count
    assert 7 == mock_mlflow_client.update_model_version.call_count


def test_import_experiment(mocker: MockerFixture):
    df: pd.DataFrame = pd.read_parquet("tests/test_recipient/experiments.parquet")

    mocker.patch("mlflow.start_run")
    mocked_set_tags = mocker.patch("mlflow.set_tags")
    mocked_log_metrics = mocker.patch("mlflow.log_metrics")
    mocked_log_params = mocker.patch("mlflow.log_params")
    mocker.patch("arcuate.recipient.write_and_log_artifacts")

    mocked_pickle = mocker.patch("cloudpickle.loads")
    mocked_pickle().metadata.flavors.__getitem__().__getitem__.return_value = "mlflow.sklearn"

    mocked_log = mocker.patch("mlflow.sklearn.log_model")

    import_experiment(df, "123")

    assert 42 == mocked_log.call_count
    assert 42 == mocked_set_tags.call_count
    assert 42 == mocked_log_metrics.call_count
    assert 84 == mocked_log_params.call_count


def test_import_experiments(mocker: MockerFixture):

    mock_mlflow_client = mlflow_setup(mocker)

    mock_experiment = mocker.patch("mlflow.create_experiment")
    mocker.patch("multiprocessing.Pool")

    df: pd.DataFrame = pd.DataFrame()
    import_experiments(mock_mlflow_client, df, "name")

    mock_experiment.assert_called_once()

    mock_experiment.side_effect = Exception()

    import_experiments(mock_mlflow_client, df, "name")

    mock_mlflow_client.get_experiment_by_name.assert_called_once()
