from unittest.mock import ANY, MagicMock, patch

import pytest
from azure.storage.blob import BlobServiceClient

from vortex.engine.blob_storage import BlobStorageEngine

# Dummy parameters for the BlobStorageEngine
blob_storage_params = {
    "storage_account_url": "https://account.blob.core.windows.net",
    "storage_account_key": "fake_key",
    "container_name": "fake_container",
    "blob_path": "fake_path",
    "blob_name": "fake_blob",
    "blob_type": "csv",
    "blob_separator": ",",
    "blob_name_is_python": False,
}


# Mock the BlobServiceClient
@pytest.fixture
def mock_blob_service_client():
    with patch("vortex.engine.blob_storage.BlobServiceClient") as mock_client:
        yield mock_client


# Test if BlobStorageEngine can be instantiated
def test_blob_storage_engine_instantiation(mock_blob_service_client):
    engine = BlobStorageEngine(params=blob_storage_params)
    assert isinstance(engine, BlobStorageEngine)


# Test if BlobStorageEngine can write and read batches without errors
@patch("vortex.engine.blob_storage.Files")
def test_blob_storage_engine_write_and_read_batch(mock_files, mock_blob_service_client):
    engine = BlobStorageEngine(params=blob_storage_params)

    # Mock Spark and DataFrames
    mock_spark = MagicMock()
    mock_dataframe = MagicMock()

    # Test write_batch method
    engine.write_batch(dataframe=mock_dataframe, spark=mock_spark)
    mock_files.csv.assert_called_once_with(spark=mock_spark, mode="out", params=ANY)

    # Test read_batch method
    engine.read_batch(spark=mock_spark)
    mock_files.csv.assert_called_with(
        spark=mock_spark, mode="in", params=ANY, bytes=ANY
    )
