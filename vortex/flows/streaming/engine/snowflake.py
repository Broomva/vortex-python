import logging

from vortex.datamodels.engine import Snowflake
from vortex.datamodels.pipeline import RunParameters

logger = logging.getLogger(__name__)


class SnowflakeWriteStream:
    def __init__(self, job_params: RunParameters, dataframe, spark):
        self.job_params = job_params
        self.dataframe = dataframe
        self.spark = spark

    def run(
        self,
    ):
        trigger_settings = {
            "default": {"processingTime": "0 seconds"},
            "processingTime": {
                "processingTime": self.job_params.sink_params["trigger_setting"]
            },
            "once": {"once": True},
            "availableNow": {"availableNow": True},
            "continuous": {
                "continuous": self.job_params.sink_params["trigger_setting"]
            },
        }

        trigger_options = trigger_settings.get(
            self.job_params.sink_params["trigger_type"], {"processingTime": "0 seconds"}
        )

        return (
            self.dataframe.writeStream.trigger(**trigger_options)
            .foreachBatch(self.foreach_batch_snowflake)
            .option("checkpointLocation", self.job_params.checkpoint_path)
            .queryName(self.job_params.pipeline_name)
            .start()
        )

    def foreach_batch_snowflake(self, dataframe, batch_id):
        params = Snowflake(**self.job_params.sink_params)
        options = {
            "sfUrl": params.url,
            "sfUser": params.user,
            "sfPassword": params.password,
            "sfDatabase": params.database,
            "sfSchema": params.scheme,
            "sfWarehouse": params.warehouse,
            "sfRole": params.role,
        }
        print(f"Running Delta Delete Stream Query using options: {options}")
        dataframe.createOrReplaceTempView("source")

        logger.info(
            f"Inserting Data into {params.database}.{params.scheme}.{params.table} in snowflake, "
            f"with user: {params.user}, role: {params.role}, and wh: {params.warehouse}"
        )

        return (
            dataframe.write.format("snowflake")
            .options(**options)
            .option("dbtable", f"{params.table}")
            .mode(params.mode)
            .save()
        )
