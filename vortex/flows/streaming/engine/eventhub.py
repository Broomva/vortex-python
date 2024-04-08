import datetime
import json
import re

from vortex.datamodels.engine import Eventhub
from vortex.datamodels.pipeline import RunParameters


def iso8601_to_unix_epoch(iso8601_str):
    """Converts an ISO 8601 string to a Unix epoch timestamp integer."""
    dt = datetime.datetime.fromisoformat(iso8601_str[:-1])
    return int(dt.timestamp())


def eh_read_stream(
    job_params: RunParameters,
    spark,
):
    """
    The function takes a StreamInstance object as an input and returns a Spark structured streaming
    dataframe with the data from the event hubs.
    Function to read data from an Azure Event Hubs stream. The function is designed to be used in a databricks notebook.

    :return: A Spark structured streaming dataframe with the data from the event hubs
    """

    source_params = job_params.source_params
    params = Eventhub(**source_params)

    sc = spark._sc
    encrypted_connection_string = (
        sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(
            params.connection_string
        )
    )

    conf = {
        "eventhubs.consumerGroup": params.consumer_group,
        "eventhubs.connectionString": encrypted_connection_string,
    }

    if params.max_events_per_trigger:
        conf["maxEventsPerTrigger"] = params.max_events_per_trigger

    if params.start_time:
        startingEventPosition = {
            "offset": None,
            "seqNo": -1,  # not in use
            "enqueuedTime": params.start_time,
            "isInclusive": True,
        }
        conf["eventhubs.startingPosition"] = json.dumps(startingEventPosition)

    if params.end_time:
        endingEventPosition = {
            "offset": None,  # not in use
            "seqNo": -1,  # not in use
            "enqueuedTime": params.end_time,
            "isInclusive": True,
        }
        conf["eventhubs.endingPosition"] = json.dumps(endingEventPosition)

    if params.offset:
        startingEventPosition = {
            "offset": params.offset,
            "seqNo": -1,
            "enqueuedTime": None,
            "isInclusive": True,
        }
        conf["eventhubs.startingPosition"] = json.dumps(startingEventPosition)

    print(conf)

    return spark.readStream.format("eventhubs").options(**conf).load()


def eh_write_stream(job_params: RunParameters, dataframe, spark):
    """
    The function takes a Spark structured streaming dataframe as an input and writes the data to an Azure Event Hubs stream.
    Function to write data to an Azure Event Hubs stream. The functions requires a dataframe with a fully qualified body column.

    :return: None
    """
    from pyspark.sql.functions import struct, to_json

    sink_params = Eventhub(**job_params.sink_params)

    sc = spark._sc
    encrypted_connection_string = (
        sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(
            sink_params.connection_string
        )
    )

    print(f"Running Stream Write Query using options: {sink_params}")
    print(f"Checkpointing into {job_params.checkpoint_path}")

    options = {
        "eventhubs.consumerGroup": sink_params.consumer_group,
        "eventhubs.connectionString": encrypted_connection_string,
        "ignoreChanges": sink_params.ignore_changes,
        "checkpointLocation": job_params.checkpoint_path,
    }

    if sink_params.max_events_per_trigger:
        options["maxEventsPerTrigger"] = sink_params.max_events_per_trigger

    payload = (
        dataframe
        # .withColumn("body", to_json(struct(list(dataframe.schema.fieldNames()))))
        .select("body")
    )

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
        payload.writeStream.trigger(**trigger_options)
        .format("eventhubs")
        .options(**options)
        .queryName(job_params.pipeline_name)
        .start()
    )


def eh_kafka_read_stream(
    job_params: RunParameters,
    spark,
):
    """
    The function takes a StreamInstance object as an input and returns a Spark structured streaming
    dataframe with the data from the event hubs.
    Function to read data from an Azure Event Hubs stream. The function is designed to be used in a databricks notebook.

    :return: A Spark structured streaming dataframe with the data from the event hubs
    """

    source_params = job_params.source_params
    params = Eventhub(**source_params)

    fqdn_pattern = re.compile("(?<=sb:\/\/)[a-zA-Z0-9\-\.]+")
    bootstrap_servers = (
        f"{str(re.search(fqdn_pattern, params.connection_string)[0])}:9093"
    )

    eventhub_name_pattern = re.compile(r"EntityPath=([^;]*)")
    topic = str(re.search(eventhub_name_pattern, params.connection_string)[1])

    trim_cs = re.sub(r";EntityPath=[a-zA-Z0-9\-]+", "", params.connection_string)
    eh_sasl = f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{trim_cs}";'

    options = {
        "subscribe": topic,
        "kafka.bootstrap.servers": bootstrap_servers,
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.jaas.config": eh_sasl,
        "kafka.request.timeout.ms": "60000",
        "kafka.session.timeout.ms": "60000",
        "failOnDataLoss": "false",
        "spark.streaming.kafka.allowNonConsecutiveOffsets": "true",
    }

    options_to_set = {
        "maxOffsetsPerTrigger": params.max_events_per_trigger,
        "startingTimestamp": iso8601_to_unix_epoch(params.start_time)
        if params.start_time
        else None,
        "endingTimestamp": iso8601_to_unix_epoch(params.end_time)
        if params.end_time
        else None,
        "startingOffsets": params.offset,
    }

    for option, value in options_to_set.items():
        if value:  # if value is not None
            options[option] = value  # set the option

    print(f"Running Eventhubs Kafka Read Stream with options: {options}")

    return spark.readStream.format("kafka").options(**options).load()
