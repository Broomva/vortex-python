from unittest.mock import MagicMock

import pytest

from vortex.datamodels.engine import Delta as DeltaParams
from vortex.engine.delta import DeltaEngine

delta_params = {
    "database": "test_database",
    "table": "test_table",
    "mode": "overwrite",
    "partition_by": "column1,column2",
    "columns": "*",
    "filter": "",
}


def test_delta_engine_init():
    delta_engine = DeltaEngine(params=delta_params)
    assert isinstance(delta_engine._params, DeltaParams)


@pytest.mark.parametrize("mode", ["overwrite", "append"])
def test_delta_engine_write_batch(mode):
    delta_engine = DeltaEngine(params=delta_params)
    mock_spark = MagicMock()
    mock_dataframe = MagicMock()

    mock_write = MagicMock()
    mock_dataframe.write = mock_write

    delta_engine._params.mode = mode
    delta_engine.write_batch(dataframe=mock_dataframe, spark=mock_spark)

    mock_write.format.assert_called_once_with("delta")
    mock_write.format.return_value.mode.assert_called_once_with(mode)


def test_delta_engine_read_batch():
    delta_engine = DeltaEngine(params=delta_params)
    mock_spark = MagicMock()

    delta_engine.read_batch(spark=mock_spark)
    mock_spark.sql.assert_called_once()


def test_delta_engine_read_stream():
    delta_engine = DeltaEngine(params=delta_params)
    mock_spark = MagicMock()

    mock_read_stream = MagicMock()
    mock_spark.readStream = mock_read_stream

    delta_engine.read_stream(spark=mock_spark)
    mock_read_stream.format.assert_called_once_with("delta")
    mock_read_stream.format.return_value.table.assert_called_once()
