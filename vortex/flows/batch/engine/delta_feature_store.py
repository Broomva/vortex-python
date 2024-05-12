from databricks.feature_store.client import FeatureStoreClient
from vortex.datamodels.engine import DeltaFeatureStoreParams
from vortex.engine import Engine
import logging

logger = logging.getLogger(__name__)


class DeltaFeatureStoreEngine(Engine):
    def __init__(self, params: dict) -> None:
        self._params = DeltaFeatureStoreParams(**params)
        self.fs = FeatureStoreClient()

    def read_stream(self, spark):
        pass

    def write_stream(self, dataframe, stage, batch_id, app_id, spark):
        pass

    def read_batch(self, spark=None):
        logger.info(f"Running Batch Read Query using options:  {self._params}")
        return self.fs.read_table(name=f"{self._params.database}.{self._params.table}")

    def write_batch(self, dataframe, spark=None) -> None:
        logger.info(f"Running Batch Write Query using options:  {self._params}")

        execution_mode = self._params.execution_mode
        mode_actions = {
            "write": self.write_feature_table,
            "create": self.create_feature_table,
            "register": self.register_feature_table,
        }

        if execution_mode in mode_actions:
            mode_actions[execution_mode](dataframe)
        else:
            raise ValueError(f"Unsupported execution mode: {execution_mode}")

    def write_feature_table(self, dataframe, spark=None) -> None:
        self.fs.write_table(
            name=f"{self._params.database}.{self._params.table}",
            df=dataframe,
            mode=self._params.mode,
        )

    def create_feature_table(self, dataframe):
        return self.fs.create_table(
            name=f"{self._params.database}.{self._params.table}",
            primary_keys=self._params.primary_keys,
            timestamp_keys=self._params.timestamp_keys,
            df=dataframe,
            description=self._params.description,
            tags=self._params.tags,
        )

    def register_feature_table(self, dataframe):
        return self.fs.register_table(
            delta_table=f"{self._params.database}.{self._params.table}",
            primary_keys=self._params.primary_keys,
            timestamp_keys=self._params.timestamp_keys,
            description=self._params.description,
            tags=self._params.tags,
        )
