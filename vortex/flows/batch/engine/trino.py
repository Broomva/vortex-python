from vortex.engine import Engine
from vortex.datamodels.engine import Trino

import logging

logger = logging.getLogger(__name__)


class TrinoEngine(Engine):
    def __init__(self, params: dict) -> None:
        self._params = Trino(**params)

    def read_batch(self, spark):
        options = {
            "driver": self._params.driver,
            "url": f"jdbc:trino://{self._params.host}:{self._params.port}/{self._params.catalog}/{self._params.scheme}",
            "query": self._params.query,
            "user": self._params.user,
            "password": self._params.password,
        }

        if self._params.ssl:
            ssl_option = {
                "SSL": bool(self._params.ssl),
                "SSLVerification": self._params.ssl_mode,
                "SSLKeyStorePath": self._params.ssl_key_store_path,
            }
            options.update(ssl_option)
        logger.info(f"Running Trino Batch Read Query using options: {options}")
        return spark.read.format("jdbc").options(**options).load()

    def write_batch(self):
        ...

    def read_stream(self):
        ...

    def write_stream(self):
        ...
