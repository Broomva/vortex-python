from vortex.datamodels.pipeline import RunParameters
from vortex.flows.streaming.engine.adls import *
from vortex.flows.streaming.engine.delta import *
from vortex.flows.streaming.engine.eventhub import *
from vortex.flows.streaming.engine.ftp import *
from vortex.flows.streaming.engine.postgres import *
from vortex.flows.streaming.engine.snowflake import *
from vortex.flows.processing import transformation
from vortex.utils.dataframe_utils import refresh_delta_table


# Stream Jobs
def eventhub_to_delta(job_params: RunParameters, spark):
    print(f"Running Stream Read Query using options: {job_params}")
    _delta_refresh = (
        refresh_delta_table(job_params, spark)
        if job_params.additional_params.get("refresh_table", False) in (True, "True")
        else None
    )
    dataframe = eh_read_stream(job_params, spark)
    transformed_dataframe = transformation(
        dataframe, spark, job_params.transformation_params
    )
    write_dataframe = delta_write_stream(job_params, transformed_dataframe, spark)
    return write_dataframe


def eventhub_kafka_to_delta(job_params: RunParameters, spark):
    print(f"Running Stream Read Query using options: {job_params}")
    _delta_refresh = (
        refresh_delta_table(job_params, spark)
        if job_params.additional_params.get("refresh_table", False) in (True, "True")
        else None
    )
    dataframe = eh_kafka_read_stream(job_params, spark)
    transformed_dataframe = transformation(
        dataframe, spark, job_params.transformation_params
    )
    write_dataframe = delta_write_stream(job_params, transformed_dataframe, spark)
    return write_dataframe


def delta_to_eventhub(job_params: RunParameters, spark):
    print(f"Running Stream Read Query using options: {job_params}")
    dataframe = delta_read_stream(job_params, spark)
    transformed_dataframe = transformation(
        dataframe, spark, job_params.transformation_params
    )
    write_dataframe = eh_write_stream(job_params, transformed_dataframe, spark)
    return write_dataframe


def delta_to_delta(job_params: RunParameters, spark):
    print(f"Running Stream Read Query using options: {job_params}")
    _delta_refresh = (
        refresh_delta_table(job_params, spark)
        if job_params.additional_params.get("refresh_table", False) in (True, "True")
        else None
    )
    dataframe = delta_read_stream(job_params, spark)
    transformed_dataframe = transformation(
        dataframe, spark, job_params.transformation_params
    )
    write_dataframe = delta_write_stream(job_params, transformed_dataframe, spark)
    return write_dataframe


def adls_to_delta(job_params: RunParameters, spark):
    print(f"Running Stream Read Query using options: {job_params}")
    _delta_refresh = (
        refresh_delta_table(job_params, spark)
        if job_params.additional_params.get("refresh_table", False) in (True, "True")
        else None
    )
    dataframe = adls_delta_read_stream(job_params, spark)
    transformed_dataframe = transformation(
        dataframe, spark, job_params.transformation_params
    )
    write_dataframe = delta_write_stream(job_params, transformed_dataframe, spark)
    return write_dataframe


def delta_to_adls(job_params: RunParameters, spark):
    print(f"Running Stream Read Query using options: {job_params}")
    dataframe = delta_read_stream(job_params, spark)
    transformed_dataframe = transformation(
        dataframe, spark, job_params.transformation_params
    )
    write_dataframe = adls_delta_write_stream(job_params, transformed_dataframe, spark)
    return write_dataframe


def adls_to_adls(job_params: RunParameters, spark):
    print(f"Running Stream Read Query using options: {job_params}")
    dataframe = adls_delta_read_stream(job_params, spark)
    transformed_dataframe = transformation(
        dataframe, spark, job_params.transformation_params
    )
    write_dataframe = adls_delta_write_stream(job_params, transformed_dataframe, spark)
    return write_dataframe


# Stream ForeachBatch Jobs


def delta_to_postgres(job_params: RunParameters, spark):
    print(f"Running Stream Read Query using options: {job_params}")
    dataframe = delta_read_stream(job_params, spark)
    transformed_dataframe = transformation(
        dataframe, spark, job_params.transformation_params
    )
    return PostgresWriteStream(job_params, transformed_dataframe, spark).run()


def delta_to_delete_stream(job_params: RunParameters, spark):
    print(f"Running Stream Read Query using options: {job_params}")
    dataframe = delta_read_stream(job_params, spark)
    return DeltaDeleteStream(job_params, dataframe, spark).run()


def delta_to_ftp(job_params: RunParameters, spark):
    print(f"Running Stream Read Query using options: {job_params}")
    dataframe = delta_read_stream(job_params, spark)
    transformed_dataframe = transformation(
        dataframe, spark, job_params.transformation_params
    )
    return FTPWriteStream(job_params, transformed_dataframe, spark).run()


def delta_to_snowflake(job_params: RunParameters, spark):
    print(f"Running Stream Read Query using options: {job_params}")
    dataframe = delta_read_stream(job_params, spark)
    transformed_dataframe = transformation(
        dataframe, spark, job_params.transformation_params
    )
    return SnowflakeWriteStream(job_params, transformed_dataframe, spark).run()


def eventhub_to_postgres(job_params: RunParameters, spark):
    print(f"Running Stream Read Query using options: {job_params}")
    _delta_refresh = (
        refresh_delta_table(job_params, spark)
        if job_params.additional_params.get("refresh_table", False) in (True, "True")
        else None
    )
    dataframe = eh_read_stream(job_params, spark)
    transformed_dataframe = transformation(
        dataframe, spark, job_params.transformation_params
    )
    return PostgresWriteStream(job_params, transformed_dataframe, spark).run()