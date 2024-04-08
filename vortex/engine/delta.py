from vortex.engine import Engine
from vortex.datamodels.engine import Delta as DeltaParams

import logging

logger = logging.getLogger(__name__)


class DeltaEngine(Engine):
    def __init__(self, params: dict) -> None:
        self._params = DeltaParams(**params)

    def write_stream(self, dataframe, stage, batch_id, app_id, spark):
        logger.info(f"Running Stream Write Query using options: {self._params}")

        if "_imp" in stage:
            table_name = f"""{self._params.table}_{stage.replace('_imp', '')}"""
        else:
            table_name = self._params.table

        return (
            dataframe.write.mode("append")
            .format("delta")
            .option("txnVersion", batch_id)
            .option("txnAppId", app_id)
            .saveAsTable(f"{self._params.database}.{table_name}")
        )

    def write_batch(self, dataframe, spark=None) -> None:
        logger.info(f"Running Batch Write Query using options:  {self._params}")
        _partition_by = (
            self._params.partition_by.split(",") if self._params.partition_by else []
        )
        (
            dataframe.write.format("delta")
            .mode(self._params.mode)
            .partitionBy(_partition_by)
            .saveAsTable(f"""{self._params.database}.{self._params.table}""")
        )

    def read_batch(self, spark):
        logger.info(f"Running Batch Read Query using options: {self._params}")
        # return spark.read.table(f"{self._params.database}.{self._params.table}")
        return spark.sql(
            f"""SELECT {self._params.columns} FROM {self._params.database}.{self._params.table} {self._params.filter}"""
        )

    def read_stream(self, spark):
        logger.info(f"Running Stream Read Query using options: {self._params}")
        return spark.readStream.format("delta").table(
            f"{self._params.database}.{self._params.table}"
        )
