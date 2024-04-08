from typing import List, Optional

from pyspark.sql import SparkSession
from pyspark.sql.types import *

import hashlib
import os
from os import walk
from pathlib import Path
from time import time


# TODO: Encapsulate this in a class


def configure_spark_with_delta_pip(
    spark_session_builder: SparkSession.Builder,
    extra_packages: Optional[List[str]] = None,
) -> SparkSession.Builder:
    """
    Utility function to configure a SparkSession builder such that the generated SparkSession
    will automatically download the required Delta Lake JARs from Maven. This function is
    required when you want to
    1. Install Delta Lake locally using pip, and
    2. Execute your Python code using Delta Lake + Pyspark directly, that is, not using
       `spark-submit --packages io.delta:...` or `pyspark --packages io.delta:...`.
        builder = SparkSession.builder \
            .master("local[*]") \
            .appName("test")
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
    3. If you would like to add more packages, use the `extra_packages` parameter.
        builder = SparkSession.builder \
            .master("local[*]") \
            .appName("test")
        my_packages = ["org.apache.spark:spark-sql-kafka-0-10_2.12:x.y.z"]
        spark = configure_spark_with_delta_pip(builder, extra_packages=my_packages).getOrCreate()
    :param spark_session_builder: SparkSession.Builder object being used to configure and
                                  create a SparkSession.
    :param extra_packages: Set other packages to add to Spark session besides Delta Lake.
    :return: Updated SparkSession.Builder object
    .. versionadded:: 1.0
    .. note:: Evolving
    """
    import importlib_metadata  # load this library only when this function is called

    if type(spark_session_builder) is not SparkSession.Builder:
        msg = f"""
This function must be called with a SparkSession builder as the argument.
The argument found is of type {str(type(spark_session_builder))}.
See the online documentation for the correct usage of this function.
        """
        raise TypeError(msg)

    try:
        delta_version = importlib_metadata.version("delta_spark")
    except Exception as e:
        msg = """
This function can be used only when Delta Lake has been locally installed with pip.
See the online documentation for the correct usage of this function.
        """
        raise Exception(msg) from e

    scala_version = "2.12"
    maven_artifact = f"io.delta:delta-core_{scala_version}:{delta_version}"

    extra_packages = extra_packages if extra_packages is not None else []
    all_artifacts = [maven_artifact] + extra_packages
    packages_str = ",".join(all_artifacts)

    return spark_session_builder.config("spark.jars.packages", packages_str)


def spark_session():
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.master("local")
        .appName("vortex_flows")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    packages = [
        "com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.21",
        "org.postgresql:postgresql:42.3.1",
    ]
    spark = configure_spark_with_delta_pip(
        spark_session_builder=builder, extra_packages=packages
    ).getOrCreate()
    import delta as delta

    sc = spark._sc
    return spark, sc


def set_local_runtime_options(spark):
    spark.conf.set("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")