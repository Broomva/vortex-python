# import pytest
# from unittest.mock import MagicMock, patch
# from vortex.engine.eventhub import EventHubEngine, BlobEventhubAutoloaderEngine
# from pyspark.sql import SparkSession
#
# @pytest.fixture(scope="module")
# def eventhub_engine():
#     params = {
#         "connection_string": "test-connection-string",
#         "consumer_group": "test-consumer-group",
#     }
#     return EventHubEngine(params)
#
# @pytest.fixture(scope="module")
# def blob_eventhub_autoloader_engine():
#     params = {
#         "container": "test-container",
#         "storage_account_name": "test-storage-account-name",
#         "storage_account_key": "test-storage-account-key",
#         "namespace": "test-namespace",
#         "eventhub": "test-eventhub",
#         "format": "test-format",
#     }
#     return BlobEventhubAutoloaderEngine(params)
#
# def test_eventhub_read_stream(eventhub_engine):
#     mock_spark = MagicMock(spec=SparkSession)
#     mock_reader = MagicMock()
#
#     with patch.object(mock_spark, "readStream", new_callable=MagicMock(return_value=mock_reader)):
#         mock_reader.format.return_value = mock_reader
#         mock_reader.options.return_value = mock_reader
#         mock_reader.load.return_value = MagicMock()
#
#         eventhub_engine.read_stream(mock_spark)
#
#     mock_reader.format.assert_called_once_with("eventhubs")
#     mock_reader.options.assert_called_once()
#     mock_reader.load.assert_called_once()
#
# def test_blob_eventhub_autoloader_read_stream(blob_eventhub_autoloader_engine):
#     mock_spark = MagicMock(spec=SparkSession)
#     mock_reader = MagicMock()
#
#     with patch.object(mock_spark, "readStream", new_callable=MagicMock(return_value=mock_reader)):
#         mock_reader.format.return_value = mock_reader
#         mock_reader.option.return_value = mock_reader
#         mock_reader.load.return_value = MagicMock()
#
#         blob_eventhub_autoloader_engine.read_stream(mock_spark)
#
#     mock_reader.format.assert_called_once_with("cloudFiles")
#     mock_reader.option.assert_called()
#     mock_reader.load.assert_called_once()
