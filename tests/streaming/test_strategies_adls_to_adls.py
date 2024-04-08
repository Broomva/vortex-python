import findspark

findspark.init()

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (FloatType, IntegerType, StructField, StructType,
                               TimestampType)

from vortex.datamodels.pipeline import RunParameters
from vortex.flows.streaming.strategies import adls_to_adls

spark = SparkSession.builder.master("local").appName("Test").getOrCreate()

schema = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("value", FloatType(), True),
    ]
)

data = [(1, datetime.now(), 1.0), (2, datetime.now(), 1.5), (3, datetime.now(), 2.0)]

df = spark.createDataFrame(data, schema)

job_params = RunParameters(
    pipeline_name="test_pipeline",
    pipeline_type="test_type",
    app_id="test_app_id",
    stage="test_stage",
    quality="test_quality",
    checkpoint_path="test_checkpoint_path",
    transformation_database="test_transformation_database",
    additional_params={"param1": "value1"},
    source_engine="eventhub",
    source_params={"param1": "value1"},
    sink_engine="delta",
    sink_params={"param1": "value1"},
    qa_params={},
    transformation_params={
        "logic_type": "sql",
        "business_logic": "SELECT * FROM source",
    },
    databricks_workspace="test_databricks_workspace",
    job_id="test_job_id",
    steps="test_steps",
)


@patch("vortex.flows.streaming.strategies.adls_delta_read_stream", new_callable=MagicMock)
@patch("vortex.flows.streaming.strategies.transformation", new_callable=MagicMock)
@patch("vortex.flows.streaming.strategies.adls_delta_write_stream", new_callable=MagicMock)
def test_adls_to_adls(
    mock_delta_write_stream, mock_transformation, mock_eh_read_stream
):
    mock_eh_read_stream.return_value = df
    mock_transformation.return_value = df
    mock_delta_write_stream.return_value = df

    result = adls_to_adls(job_params, spark)

    mock_eh_read_stream.assert_called_once_with(job_params, spark)
    mock_transformation.assert_called_once_with(
        df, spark, job_params.transformation_params
    )
    mock_delta_write_stream.assert_called_once_with(job_params, df, spark)

    assert result == df


@patch("vortex.flows.streaming.strategies.adls_delta_read_stream", new_callable=MagicMock)
def test_adls_to_adls_with_read_stream_error(mock_eh_read_stream):
    mock_eh_read_stream.side_effect = Exception("Test exception")

    with pytest.raises(Exception) as e_info:
        result = adls_to_adls(job_params, spark)

    assert str(e_info.value) == "Test exception"


@patch("vortex.flows.streaming.strategies.adls_delta_read_stream", new_callable=MagicMock)
@patch("vortex.flows.streaming.strategies.transformation", new_callable=MagicMock)
@patch("vortex.flows.streaming.strategies.adls_delta_write_stream", new_callable=MagicMock)
def test_adls_to_adls_with_empty_data(
    mock_delta_write_stream, mock_transformation, mock_eh_read_stream
):
    empty_df = spark.createDataFrame([], schema)
    mock_eh_read_stream.return_value = empty_df
    mock_transformation.return_value = empty_df
    mock_delta_write_stream.return_value = empty_df

    result = adls_to_adls(job_params, spark)

    assert result.count() == 0
