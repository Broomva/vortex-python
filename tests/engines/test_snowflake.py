from unittest.mock import MagicMock, PropertyMock, patch

import pytest
from pyspark.sql import SparkSession

from vortex.engine.snowflake import SnowflakeEngine


@pytest.fixture(scope="module")
def snowflake_engine():
    params = {
        "url": "test-url",
        "user": "test-user",
        "password": "test-password",
        "database": "test-database",
        "scheme": "test-scheme",
        "table": "test-table",
        "warehouse": "test-warehouse",
        "role": "test-role",
        "columns": "*",
        "mode": "overwrite",
    }
    return SnowflakeEngine(params)


def test_read_batch(snowflake_engine):
    mock_spark = MagicMock(spec=SparkSession)
    mock_reader = MagicMock()

    with patch.object(
        mock_spark, "read", new_callable=PropertyMock(return_value=mock_reader)
    ):
        mock_reader.format.return_value = mock_reader
        mock_reader.options.return_value = mock_reader
        mock_reader.option.return_value = mock_reader
        mock_reader.load.return_value = MagicMock()

        snowflake_engine.read_batch(mock_spark)

    mock_reader.format.assert_called_once_with("snowflake")
    mock_reader.options.assert_called_once()
    mock_reader.option.assert_called_once()
    mock_reader.load.assert_called_once()


def test_write_batch(snowflake_engine):
    mock_dataframe = MagicMock()
    mock_writer = MagicMock()

    with patch.object(
        mock_dataframe, "write", new_callable=PropertyMock(return_value=mock_writer)
    ):
        mock_writer.format.return_value = mock_writer
        mock_writer.options.return_value = mock_writer
        mock_writer.option.return_value = mock_writer
        mock_writer.mode.return_value = mock_writer
        mock_writer.save.return_value = MagicMock()

        snowflake_engine.write_batch(mock_dataframe)

    mock_writer.format.assert_called_once_with("snowflake")
    mock_writer.options.assert_called_once()
    mock_writer.option.assert_called_once()
    mock_writer.mode.assert_called_once_with(snowflake_engine._params.mode)
    mock_writer.save.assert_called_once()
