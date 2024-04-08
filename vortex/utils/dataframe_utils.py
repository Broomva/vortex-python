from vortex.datamodels.pipeline import RunParameters


def select_distinct(df, column_name):
    return list(df.select(column_name).distinct().collect())


def display_df(df, rows: int = 20):
    return df.limit(rows).toPandas().head(rows)

def refresh_delta_table(job_params: RunParameters, spark):
    from pyspark.sql.utils import AnalysisException

    try:
        refresh_type = job_params.additional_params.get("refresh_type", "truncate")
        if refresh_type not in ("truncate", "drop"):
            raise ValueError(
                f"refresh_type must be either 'truncate' or 'drop'. Got {refresh_type}"
            )
    except ValueError:
        print(f"{refresh_type=} not found in additional_params. Defaulting to truncate")
        refresh_type = "truncate"
    refresh_query = f"{refresh_type} table {job_params.sink_params['database']}.{job_params.sink_params['table']}"
    print(f"Refresh is set to True. Running Query: {refresh_query}")
    try:
        spark.sql(refresh_query)
    except AnalysisException as e:
        print(f"Could not refresh table. {e}. Continuing with the job")
        return None
