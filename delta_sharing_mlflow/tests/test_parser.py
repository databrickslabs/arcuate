import pytest

from delta_sharing_mlflow.parser import arcuate_parse


def test_parse_export_experiment():
    tokens = arcuate_parse(
        "create share 'ml_sharing' with table 'table_id' from experiment 'experiment_id'"
    )
    assert tokens == ["ml_sharing", "table_id", "experiment_id"]


def test_parse_export_model():
    tokens = arcuate_parse(
        "create share 'ml_sharing' with table 'model_table' from model 'income-prediction-model'"
    )
    assert tokens == ["ml_sharing", "model_table", "model", "income-prediction-model"]


def test_parse_import_experiment():
    tokens = arcuate_parse("create experiment 'experiment_id' as pandas 'delta_share#coordinate'")
    assert tokens == ["experiment_id", "delta_share#coordinate"]


def test_parse_import_experiment_overwrite():
    tokens = arcuate_parse(
        "create experiment overwrite 'experiment_id' as pandas 'delta_share#coordinate'"
    )
    assert tokens == ["experiment_id", "delta_share#coordinate"]


def test_parse_import_model():
    tokens = arcuate_parse("create model overwrite 'ml_sharing' as pandas 'delta_share#coordinate'")
    assert tokens == ["model", "ml_sharing", "delta_share#coordinate"]


def test_parse_import_model_overwrite():
    tokens = arcuate_parse("create model overwrite 'ml_sharing' as pandas 'delta_share#coordinate'")
    assert tokens == ["model", "ml_sharing", "delta_share#coordinate"]


def test_invalid_parser():
    with pytest.raises(NotImplementedError):
        _ = arcuate_parse(
            "create table 'ml_sharing' with table 'table_id' from experiment 'experiment_id'"
        )
