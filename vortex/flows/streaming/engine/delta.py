import time

from vortex.datamodels.engine import Delta
from vortex.datamodels.pipeline import RunParameters


def delta_read_stream(job_params: RunParameters, spark):
    source_params = Delta(**job_params.source_params)
    print(f"Running Stream Read Query using options: {source_params}")
    options = {"ignoreChanges": True, "ignoreDeletes": True}

    if source_params.starting_version:
        options["startingVersion"] = source_params.starting_version

    if source_params.starting_timestamp:
        options["startingTimestamp"] = source_params.starting_timestamp

    if source_params.max_files_per_trigger:
        options["maxFilesPerTrigger"] = source_params.max_files_per_trigger

    if source_params.max_bytes_per_trigger:
        options["maxBytesPerTrigger"] = source_params.max_bytes_per_trigger

    return (
        spark.readStream.format("delta")
        .options(**options)
        .table(f"{source_params.database}.{source_params.table}")
    )


def delta_write_stream(job_params: RunParameters, dataframe, spark):
    sink_params = Delta(**job_params.sink_params)
    _partition_by = (
        sink_params.partition_by.split(",") if sink_params.partition_by else []
    )

    print(f"Running Stream Write Query using options: {sink_params}")
    print(f"Checkpointing into {job_params.checkpoint_path}")

    options = {
        "ignoreChanges": sink_params.ignore_changes,
        "ignoreDeletes ": True,
        "checkpointLocation": job_params.checkpoint_path,
    }

    trigger_settings = {
        "default": {"processingTime": "0 seconds"},
        "processingTime": {"processingTime": sink_params.trigger_setting},
        "once": {"once": True},
        "availableNow": {"availableNow": True},
        "continuous": {"continuous": sink_params.trigger_setting},
    }

    trigger_options = trigger_settings.get(
        sink_params.trigger_type, {"processingTime": "0 seconds"}
    )

    return (
        dataframe.writeStream.trigger(**trigger_options)
        .format("delta")
        .outputMode(sink_params.mode)
        .options(**options)
        .partitionBy(_partition_by)
        .queryName(job_params.pipeline_name)
        .toTable(f"{sink_params.database}.{sink_params.table}")
    )


class DeltaDeleteStream:
    def __init__(self, job_params: RunParameters, dataframe, spark):
        self.job_params = job_params
        self.dataframe = dataframe
        self.spark = spark

    def run(
        self,
    ):
        # sleep for 2 minutes to allow the delta table to be created
        time.sleep(120)
        return (
            self.dataframe.writeStream.format("delta")
            .outputMode("append")
            .foreachBatch(self.foreach_batch_delta)  # Use qa function
            .option("checkpointLocation", self.job_params.checkpoint_path)
            .queryName(self.job_params.pipeline_name)
            .start()
        )

    def foreach_batch_delta(self, dataframe, batch_id):
        from pyspark.sql.functions import current_timestamp

        params = Delta(**self.job_params.sink_params)
        options = {"ignoreChanges": True, "ignoreDeletes": True}
        print(f"Running Delta Delete Stream Query using options: {options}")
        transformation = self.job_params.transformation_params
        dataframe.createOrReplaceTempView("source")

        return (
            self.spark.sql(transformation["business_logic"])
            # .withColumn("delete_timestamp", current_timestamp())
            # .write
            # .format("delta")
            # .mode(params.mode)
            # .options(**options)
            # .saveAsTable(f"{params.database}.{params.table}")
        )
