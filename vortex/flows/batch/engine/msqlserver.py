from vortex.engine import Engine
from vortex.datamodels.engine import MSQLServer

import logging

logger = logging.getLogger(__name__)


class MSQLServerEngine(Engine):
    def __init__(self, params: dict) -> None:
        self._params = MSQLServer(**params)

    def write_stream(self, dataframe, stage, batch_id, app_id, spark) -> None:
        pass

    def read_stream(self, spark):
        pass

    def write_batch(self, dataframe, spark=None) -> None:
        options = {
            "url": f"""jdbc:sqlserver://{self._params.host}:{self._params.port};database={self._params.database}""",
            "dbtable": self._params.table,
            "user": self._params.user,
            "password": self._params.password,
        }
        if self._params.mode == "overwrite":
            overwrite_options = {"truncate": self._params.clean_slate}
            options.update(overwrite_options)
        logger.info(f"Running Batch Write Query using options: {options}")
        (
            dataframe.write.mode(self._params.mode)
            .format("jdbc")
            .options(**options)
            .save()
        )
        logger.info(f"Inserted Data into {self._params.table}")

    def read_batch(self, spark):
        options = {
            "url": f"""jdbc:sqlserver://{self._params.host}:{self._params.port};database={self._params.database}""",
            "dbtable": f"""(SELECT {self._params.columns} FROM {self._params.scheme}.{self._params.table} {self._params.filter}) AS t""",
            "user": self._params.user,
            "password": self._params.password,
        }
        logger.info(f"Running Batch Read Query using options: {options}")
        dataframe = spark.read.format("jdbc").options(**options).load()
        return dataframe
