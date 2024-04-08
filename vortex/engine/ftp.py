from datetime import datetime
from io import BytesIO
from time import time

import paramiko

from vortex.engine import Engine
from vortex.datamodels.engine import FTP, File
from vortex.utils.files import Files

import logging

logger = logging.getLogger(__name__)


class FTPEngine(Engine):
    def __init__(self, params: dict) -> None:
        self._params = FTP(**params)

    def write_stream(self, dataframe, stage, batch_id, app_id, spark) -> None:
        pass

    def read_stream(self, spark):
        pass

    def write_batch(self, dataframe, spark=None) -> None:
        file_params = {
            "separator": self._params.file_separator,
        }

        params = File(**file_params)
        file = getattr(Files, self._params.file_type)(
            dataframe=dataframe, spark=spark, mode="out", params=params
        )

        transport = paramiko.Transport((self._params.host, self._params.port))
        transport.connect(username=self._params.user, password=self._params.password)

        ts = time()
        file_datetime = datetime.fromtimestamp(ts).strftime(
            self._params.file_datetime_name_format
        )

        sftp = paramiko.SFTPClient.from_transport(transport)
        file_path = f"{self._params.root}{self._params.file_name}{file_datetime}.{self._params.file_type}"

        sftp.putfo(BytesIO(file.encode()), file_path)
        sftp.close()
        transport.close()

        logger.info(f"File {file_path} uploaded to FTP server.")
        logger.info(dataframe.collect())

    def read_batch(self, spark):
        pass
