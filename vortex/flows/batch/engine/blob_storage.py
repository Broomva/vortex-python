from vortex.engine import Engine
from vortex.datamodels.engine import BlobStorage, File

from vortex.utils.files import Files
from azure.storage.blob import BlobServiceClient


class BlobStorageEngine(Engine):
    def __init__(self, params: dict) -> None:
        self._params = BlobStorage(**params)

    def write_stream(self, dataframe, stage, batch_id, app_id, spark) -> None:
        pass

    def read_stream(self, spark):
        pass

    def write_batch(self, dataframe, spark) -> None:
        blob_name = f"""{self._params.blob_path}{'/' + eval(self._params.blob_name) if self._params.blob_name_is_python else self._params.blob_name}.{self._params.blob_type} """

        file_params = {
            "separator": self._params.blob_separator,
            "type": self._params.blob_type,
            "name": blob_name,
        }

        params = File(**file_params)

        blob_service_client_instance = BlobServiceClient(
            account_url=self._params.storage_account_url,
            credential=self._params.storage_account_key,
        )

        file = getattr(Files, self._params.blob_type)(
            spark=spark, mode="out", params=params
        )

        blob_client_instance = blob_service_client_instance.get_blob_client(
            self._params.container_name, blob_name, snapshot=None
        )

        blob_client_instance.upload_blob(file)

    def read_batch(self, spark=None):
        blob_service_client_instance = BlobServiceClient(
            account_url=self._params.storage_account_url,
            credential=self._params.storage_account_key,
        )
        blob_name = f"""{self._params.blob_path}{'/' + eval(self._params.blob_name) if self._params.blob_name_is_python else self._params.blob_name}.{self._params.blob_type} """
        blob_client_instance = blob_service_client_instance.get_blob_client(
            self._params.container_name, blob_name, snapshot=None
        )

        blob_data = blob_client_instance.download_blob()

        file_params = {
            "separator": self._params.blob_separator,
            "type": self._params.blob_type,
        }

        params = File(**file_params)

        return getattr(Files, self._params.blob_type)(
            spark=spark, mode="in", params=params, bytes=blob_data
        )
