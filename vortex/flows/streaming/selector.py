from vortex.datamodels.pipeline import RunParameters
from vortex.flows.streaming.strategies import *

strategies_mapping = {
    "eventhub_to_delta": eventhub_to_delta,
    "eventhub_kafka_to_delta": eventhub_kafka_to_delta,
    "delta_to_eventhub": delta_to_eventhub,
    "delta_to_delta": delta_to_delta,
    "delta_to_postgres": delta_to_postgres,
    "delta_to_adls": delta_to_adls,
    "adls_to_delta": adls_to_delta,
    "adls_to_adls": adls_to_adls,
    "delta_to_delete_stream": delta_to_delete_stream,
    "delta_to_ftp": delta_to_ftp,
    "delta_to_snowflake": delta_to_snowflake,
    "eventhub_to_postgres": eventhub_to_postgres,
}


def selector(job_params: RunParameters, spark):
    """
    Selects the appropriate streaming strategy based on the job parameters.

    Args:
        job_params (RunParameters): The parameters for the job.
        spark: The SparkSession object.

    Returns:
        The result of the selected streaming strategy.
    """
    job_steps = job_params.steps
    print(f"Running Streaming Job for steps: {job_steps}")
    return strategies_mapping[job_steps](job_params, spark)
