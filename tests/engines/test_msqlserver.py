from unittest.mock import MagicMock, call, patch

import pytest

from vortex.engine.msqlserver import MSQLServerEngine


@pytest.fixture
def msql_server_engine():
    params = {
        "host": "localhost",
        "port": 1433,
        "database": "test_db",
        "table": "test_table",
        "user": "test_user",
        "password": "test_password",
        "scheme": "dbo",
        "columns": "*",
        "filter": "",
        "mode": "overwrite",
        "clean_slate": True,
    }
    engine = MSQLServerEngine(params)
    return engine


def test_msql_server_engine_write_batch(msql_server_engine):
    mock_dataframe = MagicMock()
    mock_writer = MagicMock()
    mock_dataframe.write.return_value = mock_writer
    mock_writer.mode.return_value = mock_writer
    mock_writer.format.return_value = mock_writer
    mock_writer.options.return_value = mock_writer

    msql_server_engine.write_batch(mock_dataframe)

    mock_dataframe.write().mode("overwrite").format("jdbc").options(
        url="jdbc:sqlserver://localhost:1433;database=test_db",
        dbtable="test_table",
        user="test_user",
        password="test_password",
        truncate=True,
    ).save()

    assert mock_dataframe.write.call_args_list == [call()]
    assert mock_writer.mode.call_args_list == [call("overwrite")]
    assert mock_writer.format.call_args_list == [call("jdbc")]
    assert mock_writer.options.call_args_list == [
        call(
            url="jdbc:sqlserver://localhost:1433;database=test_db",
            dbtable="test_table",
            user="test_user",
            password="test_password",
            truncate=True,
        )
    ]
    assert mock_writer.save.call_args_list == [call()]


def test_msql_server_engine_read_batch(msql_server_engine):
    mock_spark = MagicMock()
    mock_reader = MagicMock()
    mock_spark.read.return_value = mock_reader
    mock_reader.format.return_value = mock_reader
    mock_reader.options.return_value = mock_reader

    msql_server_engine.read_batch(mock_spark)

    mock_spark.read().format("jdbc").options(
        url="jdbc:sqlserver://localhost:1433;database=test_db",
        dbtable="(SELECT * FROM dbo.test_table ) AS t",
        user="test_user",
        password="test_password",
    ).load()

    assert mock_spark.read.call_args_list == [call()]
    assert mock_reader.format.call_args_list == [call("jdbc")]
    assert mock_reader.options.call_args_list == [
        call(
            url="jdbc:sqlserver://localhost:1433;database=test_db",
            dbtable="(SELECT * FROM dbo.test_table ) AS t",
            user="test_user",
            password="test_password",
        )
    ]
    assert mock_reader.load.call_args_list == [call()]
