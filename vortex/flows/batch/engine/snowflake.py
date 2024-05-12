import logging

from vortex.engine import Engine
from vortex.datamodels.engine import Snowflake

logger = logging.getLogger(__name__)


class SnowflakeEngine(Engine):
    """
    It makes sense to use the Builder pattern only when your products are quite
    complex and require extensive configuration.

    Unlike in other creational patterns, different concrete builders can produce
    unrelated products. In other words, results of various builders may not
    always follow the same interface.

    Connector documentation https://docs.snowflake.com/en/user-guide/spark-connector-use
    """

    def __init__(self, params: dict) -> None:
        self._params = Snowflake(**params)

    def read_stream(self, spark):
        pass

    def write_stream(self, dataframe, stage, batch_id, app_id, spark) -> None:
        pass

    def read_batch(self, spark):
        options = {
            "sfUrl": self._params.url,
            "sfUser": self._params.user,
            "sfPassword": self._params.password,
            "sfDatabase": self._params.database,
            "sfSchema": self._params.scheme,
            "sfWarehouse": self._params.warehouse,
            "sfRole": self._params.role,
        }
        query = f"""SELECT {self._params.columns} FROM {self._params.database}.{self._params.scheme}.{self._params.table}"""
        logger.info(
            f"Now running query: {query} in snowflake with user: {self._params.user}, role: {self._params.role}, and "
            f"wh: {self._params.warehouse}"
        )

        return (
            spark.read.format("snowflake")
            .options(**options)
            .option("query", query)
            .load()
        )

    def write_batch(self, dataframe, spark=None) -> None:
        options = {
            "sfUrl": self._params.url,
            "sfUser": self._params.user,
            "sfPassword": self._params.password,
            "sfDatabase": self._params.database,
            "sfSchema": self._params.scheme,
            "sfWarehouse": self._params.warehouse,
            "sfRole": self._params.role,
        }
        if self._params.mode == "overwrite":
            overwrite_options = {"truncate_table": self._params.clean_slate}
            options.update(overwrite_options)
        logger.info(
            f"Inserting Data into {self._params.database}.{self._params.scheme}.{self._params.table} in snowflake, "
            f"with user: {self._params.user}, role: {self._params.role}, and wh: {self._params.warehouse}"
        )
        (
            dataframe.write.format("snowflake")
            .options(**options)
            .option("dbtable", f"{self._params.table}")
            .mode(self._params.mode)
            .save()
        )
