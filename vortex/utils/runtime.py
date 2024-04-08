from math import floor
from os import environ


def set_runtime_options(
    spark,
    cores_count=4,
    partitioning_factor=2,
    paralellism_factor=2,
    throttling_enabled=False,
    default_parallelism_enabled=True,
    auto_optimize_enabled=True,
):
    """
    > It sets the number of partitions and parallelism based on the number of cores available in the
    cluster

    :param spark: SparkSession
    :param cores_count: The number of cores in the cluster
    :param partitioning_factor: The number of partitions to use for the dataframe
    :param paralellism_factor: This is the factor by which the number of cores is multiplied to get the
    number of partitions
    :param throttling_enabled: If set to true, the number of cores will be used to set the number of
    partitions and the parallelism factor. If set to false, the number of cores will be used to set the
    number of partitions and the parallelism factor will be set to 1, defaults to True (optional)
    """

    if throttling_enabled:
        spark.conf.set(
            "spark.sql.shuffle.partitions",
            f"{floor(((1 / float(partitioning_factor)) * int(cores_count)))}",
        )  # Number of cores
        spark.conf.set(
            "spark.default.parallelism",
            f"{float(cores_count) * float(paralellism_factor)}",
        )  # f"{floor(cores_count * throttling_factor)}")  # Number of cores tripled or doubled

    if default_parallelism_enabled:
        spark.conf.set(
            "spark.sql.shuffle.partitions", spark.sparkContext.defaultParallelism
        )
    spark.conf.set(
        "spark.sql.streaming.stateStore.providerClass",
        "com.databricks.sql.streaming.state.RocksDBStateStoreProvider",
    )
    spark.conf.set("spark.sql.streaming.noDataMicroBatches.enabled", "false")
    spark.conf.set("spark.streaming.stopGracefullyOnShutdown", "true")
    spark.conf.set("spark.sql.streaming.metricsEnabled", "true")
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

    if auto_optimize_enabled:
        spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
        spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

    # spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "false")

    # spark.conf.set("spark.sql.adaptive", "enabled")
    # spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning", "enabled")
    # spark.conf.set("spark.databricks.streaming.statefulOperator.asyncCheckpoint.enabled","true")
