from vortex.engine import Engine
from vortex.datamodels.engine import ADLS as ADLSParams

import logging

logger = logging.getLogger(__name__)


class ADLSEngine(Engine):
    def __init__(self, params: dict) -> None:
        self._params = ADLSParams(**params)

    def write_stream(self, dataframe, stage, batch_id, app_id):
        pass

    def write_batch(self, dataframe, spark) -> None:
        """
        This function takes in a dataframe, and a spark session. It writes the
        dataframe to a delta table in ADLS provided the definitions on the runtime module
        """
        spark.conf.set(
            f"fs.azure.account.key.{self._params.storage_account_name}.dfs.core.windows.net",
            self._params.storage_account_key,
        )
        spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")
        adls_save_path = f"abfss://{self._params.container}@{self._params.storage_account_name}.dfs.core.windows.net/{self._params.database}/{self._params.table}"
        logger.info(f"Writing data to ADLS delta table {self._params.table}")
        (dataframe.write.mode("append").format("delta").save(adls_save_path))
        logger.info(f"Wrote data to ADLS delta table in path: {adls_save_path}")

    def read_batch(self, spark):
        spark.conf.set(
            f"fs.azure.account.key.{self._params.storage_account_name}.dfs.core.windows.net",
            self._params.storage_account_key,
        )
        spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")
        adls_save_path = f"abfss://{self._params.container}@{self._params.storage_account_name}.dfs.core.windows.net/{self._params.database}/{self._params.table}"
        return spark.read.format("delta").load(adls_save_path)

    def read_stream(self, spark):
        pass
