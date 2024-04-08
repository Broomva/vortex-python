from vortex.datamodels.engine import FTP
from vortex.datamodels.pipeline import RunParameters


class FTPWriteStream:
    def __init__(self, job_params: RunParameters, dataframe, spark):
        self.job_params = job_params
        self.dataframe = dataframe
        self.spark = spark
        self.params = FTP(**self.job_params.sink_params)

    def run(
        self,
    ):
        trigger_settings = {
            "default": {"processingTime": "0 seconds"},
            "processingTime": {"processingTime": self.params.trigger_setting},
            "once": {"once": True},
            "availableNow": {"availableNow": True},
            "continuous": {"continuous": self.params.trigger_setting},
        }

        trigger_options = trigger_settings.get(
            self.params.trigger_type, {"processingTime": "0 seconds"}
        )

        return (
            self.dataframe.writeStream.trigger(**trigger_options)
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", self.job_params.checkpoint_path)
            .queryName(self.job_params.pipeline_name)
            .foreachBatch(self.foreach_batch_delta)  # Use qa function
            .start()
        )

    def foreach_batch_delta(self, dataframe, batch_id):
        from datetime import datetime
        from io import BytesIO
        from time import time

        import paramiko
        from pyspark.sql.functions import current_timestamp

        from vortex.datamodels.engine import File
        from vortex.utils.files import Files

        file_params = {
            "separator": self.params.file_separator,
        }

        file_params = File(**file_params)

        print(
            f"Running FTP Write Stream Query using options: {self.params}, {file_params}"
        )

        file = getattr(Files, self.params.file_type)(
            dataframe=dataframe, spark=self.spark, mode="out", params=file_params
        )

        transport = paramiko.Transport((self.params.host, self.params.port))
        transport.connect(username=self.params.user, password=self.params.password)

        ts = time()
        file_datetime = datetime.fromtimestamp(ts).strftime(
            self.params.file_datetime_name_format
        )

        sftp = paramiko.SFTPClient.from_transport(transport)
        file_path = f"{self.params.root}{self.params.file_name}{file_datetime}.{self.params.file_type}"

        sftp.putfo(BytesIO(file.encode()), file_path)
        sftp.close()
        transport.close()

        print(f"File {file_path} uploaded to FTP server.")
        print(f"Wrote {dataframe.count()} rows to FTP server.")
        print(dataframe.show())
