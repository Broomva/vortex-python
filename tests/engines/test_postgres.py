from unittest.mock import MagicMock, PropertyMock

import pytest
from pyspark.sql import SparkSession

from vortex.engine.postgres import PostgresEngine


@pytest.fixture(scope="module")
def postgres_engine_params():
    return {
        "driver": "org.postgresql.Driver",
        "host": "localhost",
        "database": "test_db",
        "scheme": "public",
        "table": "test_table",
        "user": "test_user",
        "password": "test_password",
        "ssl": False,
        "mode": "overwrite",
        "clean_slate": True,
    }


@pytest.fixture(scope="module")
def postgres_engine(postgres_engine_params):
    return PostgresEngine(params=postgres_engine_params)


from unittest.mock import patch


def test_read_batch(postgres_engine):
    mock_spark = MagicMock(spec=SparkSession)
    mock_reader = MagicMock()

    with patch.object(
        mock_spark, "read", new_callable=PropertyMock(return_value=mock_reader)
    ):
        mock_reader.format.return_value = mock_reader
        mock_reader.options.return_value = mock_reader
        mock_reader.load.return_value = MagicMock()

        postgres_engine.read_batch(mock_spark)

    mock_reader.format.assert_called_once_with("jdbc")
    mock_reader.options.assert_called_once()
    mock_reader.load.assert_called_once()


def test_write_batch(postgres_engine):
    mock_dataframe = MagicMock()
    mock_writer = MagicMock()

    with patch.object(
        mock_dataframe, "write", new_callable=PropertyMock(return_value=mock_writer)
    ):
        mock_writer.mode.return_value = mock_writer
        mock_writer.format.return_value = mock_writer
        mock_writer.options.return_value = mock_writer
        mock_writer.save.return_value = MagicMock()

        postgres_engine.write_batch(mock_dataframe)

    mock_writer.mode.assert_called_once_with(postgres_engine._params.mode)
    mock_writer.format.assert_called_once_with("jdbc")
    mock_writer.options.assert_called_once()
    mock_writer.save.assert_called_once()
