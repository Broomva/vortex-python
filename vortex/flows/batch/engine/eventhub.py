from vortex.engine import Engine
from vortex.datamodels.engine import (
    BlobEventhubAutoloader as BlobEventhubAutoloaderParams,
)
from vortex.datamodels.engine import Eventhub as EventhubParams

import logging

logger = logging.getLogger(__name__)


class EventHubEngine(Engine):
    """
    It makes sense to use the Builder pattern only when your products are quite
    complex and require extensive configuration.

    Unlike in other creational patterns, different concrete builders can produce
    unrelated products. In other words, results of various builders may not
    always follow the same interface.
    """

    def __init__(self, params: dict) -> None:
        self._params = EventhubParams(**params)

    def read_stream(self, spark):
        """
        The function takes a StreamInstance object as an input and returns a Spark structured streaming
        dataframe with the data from the event hubs.
        Function to read data from an Azure Event Hubs stream. The function is designed to be used in a databricks notebook.

        :return: A Spark structured streaming dataframe with the data from the event hubs
        """
        sc = spark._sc
        encrypted_connection_string = (
            sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(
                self._params.connection_string
            )
        )
        conf = {
            "eventhubs.consumerGroup": self._params.consumer_group,
            "eventhubs.connectionString": encrypted_connection_string,
            "maxEventsPerTrigger": 262144,
        }
        return spark.readStream.format("eventhubs").options(**conf).load()

    def write_stream(self, stream_df, stage, batch_id, app_id, spark) -> None:
        pass

    def read_batch(self, spark):
        pass

    def write_batch(self, dataframe, spark=None) -> None:
        pass


class BlobEventhubAutoloaderEngine(Engine):
    def __init__(self, params: dict) -> None:
        self._params = BlobEventhubAutoloaderParams(**params)

    def write_stream(
        self, dataframe=None, stage=None, batch_id=None, app_id=None, spark=None
    ):
        pass

    def write_batch(self, dataframe, spark=None) -> None:
        pass

    def read_batch(self, spark):
        pass

    def read_stream(self, spark):
        wasbs_path = f"wasbs://{self._params.container}@{self._params.storage_account_name}.blob.core.windows.net/{self._params.namespace}/{self._params.eventhub}"
        spark.conf.set(
            f"fs.azure.account.key.{self._params.storage_account_name}.blob.core.windows.net",
            self._params.storage_account_key,
        )

        logger.info(f"Running Read Stream Query using options: {self._params}")
        return (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", self._params.format)  #'avro')
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaLocation", "dbfs:/FileStore/schema_infers")
            .option("cloudFiles.includeExistingFiles", "true")
            .load(wasbs_path)
        )
