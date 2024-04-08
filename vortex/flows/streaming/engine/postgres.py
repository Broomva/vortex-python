from pyspark.sql.functions import current_timestamp

from vortex.datamodels.engine import PostgresSQL
from vortex.datamodels.pipeline import RunParameters


class PostgresWriteStream:
    def __init__(self, job_params: RunParameters, dataframe, spark):
        self.job_params = job_params
        self.dataframe = dataframe
        self.spark = spark

    def run(
        self,
    ):
        sink_params = PostgresSQL(**self.job_params.sink_params)
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
            self.dataframe.writeStream.trigger(**trigger_options).foreachBatch(
                self.foreach_batch_postgres
            )  # Use qa function
            .option("checkpointLocation", self.job_params.checkpoint_path)
            .queryName(self.job_params.pipeline_name)
            .start()
        )

    def foreach_batch_postgres(self, dataframe, batch_id):
        params = PostgresSQL(**self.job_params.sink_params)
        options = {
            "driver": params.driver,
            "url": f"jdbc:postgresql://{params.host}/{params.database}",
            "dbtable": f"{params.scheme}.{params.table}",
            "user": params.user,
            "password": params.password,
            "txnVersion": batch_id,
            "txnAppId": self.job_params.app_id,
        }
        if params.ssl:
            ssl_option = {
                "ssl": bool(params.ssl),
                "sslmode": "require",
                "sslrootcert": "/dbfs/FileStore/db_certificates/server_ca.der",
                "trustServerCertificate": True,
            }
            options.update(ssl_option)

        if params.mode == "overwrite":
            overwrite_options = {"truncate": params.clean_slate}
            options.update(overwrite_options)

        print(f"Running Stream Write Query using options: {options}")
        dataframe = dataframe.withColumn("insert_timestamp", current_timestamp())
        # dataframe = dataframe.withColumn("batch_id", lit(batch_id))
        return (
            dataframe.write.mode(params.mode).format("jdbc").options(**options).save()
        )
