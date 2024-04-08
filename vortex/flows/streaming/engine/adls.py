from vortex.datamodels.engine import ADLS
from vortex.datamodels.pipeline import RunParameters


def adls_delta_write_stream(job_params: RunParameters, dataframe, spark):
    sink_params = ADLS(**job_params.sink_params)
    _partition_by = (
        sink_params.partition_by.split(",") if sink_params.partition_by else []
    )

    spark.conf.set(
        f"fs.azure.account.key.{sink_params.storage_account_name}.dfs.core.windows.net",
        sink_params.storage_account_key,
    )
    spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")
    adls_save_path = f"abfss://{sink_params.container}@{sink_params.storage_account_name}.dfs.core.windows.net/{sink_params.database}/{sink_params.table}"

    print(f"Running Stream Write Query using options: {sink_params}")
    print(f"Checkpointing into {job_params.checkpoint_path}")

    options = {
        # "ignoreChanges": sink_params.ignore_changes,
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
        .start(adls_save_path)
    )


def adls_delta_read_stream(job_params: RunParameters, spark):
    source_params = ADLS(**job_params.source_params)

    spark.conf.set(
        f"fs.azure.account.key.{source_params.storage_account_name}.dfs.core.windows.net",
        source_params.storage_account_key,
    )
    spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")
    adls_save_path = f"abfss://{source_params.container}@{source_params.storage_account_name}.dfs.core.windows.net/{source_params.database}/{source_params.table}"

    print(f"Running Stream Read Query using options: {source_params}")
    options = {}

    if source_params.starting_version:
        options["startingVersion"] = source_params.starting_version

    if source_params.starting_timestamp:
        options["startingTimestamp"] = source_params.starting_timestamp

    if source_params.max_files_per_trigger:
        options["maxFilesPerTrigger"] = source_params.max_files_per_trigger

    if source_params.max_bytes_per_trigger:
        options["maxBytesPerTrigger"] = source_params.max_bytes_per_trigger

    return spark.readStream.format("delta").options(**options).load(adls_save_path)
