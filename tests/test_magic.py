from arcuate.magic import ArcuateMagic
from pytest_mock import MockerFixture
import pytest


class TestArcuateMagic(ArcuateMagic):
    def __init__(self, mocker: MockerFixture):
        self.client = mocker.MagicMock()
        self.spark = mocker.MagicMock()


def test_arcuate_import_experiment(mocker: MockerFixture):
    magic = TestArcuateMagic(mocker)

    mocker.patch("delta_sharing.load_as_pandas")
    mocked_delete = mocker.patch("arcuate.magic.delete_mlflow_experiment")
    mocked_import = mocker.patch("arcuate.magic.import_experiments")

    magic.arcuate(
        "CREATE",
        "OR",
        "REPLACE",
        "EXPERIMENT",
        "experiment_name",
        "AS",
        "PANDAS",
        "delta_sharing_coordinate",
    )

    mocked_import.assert_called_once()
    mocked_delete.assert_called_once()


def test_invalid_arcuate_import_experiment(mocker: MockerFixture):
    with pytest.raises(NotImplementedError):
        magic = TestArcuateMagic(mocker)
        magic.arcuate(
            "CREATE",
            "OR",
            "REPLACE",
            "EXPERIMENT",
            "experiment_name",
            "AS",
            "SPARK",
            "delta_sharing_coordinate",
        )


def test_arcuate_import_model(mocker: MockerFixture):
    magic = TestArcuateMagic(mocker)

    mocker.patch("delta_sharing.load_as_pandas")
    mocked_delete = mocker.patch("arcuate.magic.delete_mlflow_model")
    mocked_import = mocker.patch("arcuate.magic.import_models")

    magic.arcuate(
        "CREATE",
        "OR",
        "REPLACE",
        "MODEL",
        "model",
        "AS",
        "PANDAS",
        "delta_sharing_coordinate",
    )

    mocked_import.assert_called_once()
    mocked_delete.assert_called_once()


def test_invalid_arcuate_import_model(mocker: MockerFixture):
    with pytest.raises(NotImplementedError):
        magic = TestArcuateMagic(mocker)
        magic.arcuate(
            "CREATE",
            "MODEL",
            "OVERWRITE",
            "model",
            "AS",
            "SPARK",
            "delta_sharing_coordinate",
        )


def test_arcuate_export_experiment(mocker: MockerFixture):
    magic = TestArcuateMagic(mocker)

    mocked_export = mocker.patch("arcuate.magic.export_experiments")

    magic.arcuate(
        "CREATE",
        "SHARE",
        "'share_name'",
        "WITH",
        "TABLE",
        "'table_name'",
        "FROM",
        "EXPERIMENT",
        "'experiment_name'",
    )

    mocked_export.assert_called_once()


def test_arcuate_export_model(mocker: MockerFixture):
    magic = TestArcuateMagic(mocker)

    mocked_export = mocker.patch("arcuate.magic.export_models")

    magic.arcuate(
        "CREATE",
        "SHARE",
        "'share_name'",
        "WITH",
        "TABLE",
        "'table_name'",
        "FROM",
        "MODEL",
        "'model_name'",
    )

    mocked_export.assert_called_once()
