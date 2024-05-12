from pandas import DataFrame
from psycopg2 import connect
from pyspark.sql.functions import current_timestamp, lit
from vortex.engine import Engine
from vortex.datamodels.engine import PostgresSQL


import logging

logger = logging.getLogger(__name__)


class PostgresEngine(Engine):
    def __init__(self, params: dict) -> None:
        self._params = PostgresSQL(**params)

    def write_stream(self, dataframe, stage, batch_id, app_id) -> None:
        options = {
            "driver": self._params.driver,
            "url": f"jdbc:postgresql://{self._params.host}/{self._params.database}",
            "dbtable": f"{self._params.scheme}.{self._params.table}",
            "user": self._params.user,
            "password": self._params.password,
            "txnVersion": batch_id,
            "txnAppId": app_id,
        }
        if self._params.ssl:
            ssl_option = {
                "ssl": bool(self._params.ssl),
                "sslmode": "require",
                "sslrootcert": "/dbfs/FileStore/db_certificates/server_ca.der",
                "trustServerCertificate": True,
            }
            options.update(ssl_option)

        if self._params.mode == "overwrite":
            overwrite_options = {"truncate": self._params.clean_slate}
            options.update(overwrite_options)
        logger.info(f"Running Stream Write Query using options: {options}")
        dataframe = dataframe.withColumn("insert_timestamp", current_timestamp())
        # dataframe = dataframe.withColumn("batch_id", lit(batch_id))
        return (
            dataframe.write.mode(self._params.mode)
            .format("jdbc")
            .options(**options)
            .save()
        )

    def read_stream(self, spark):
        pass

    def write_batch(self, dataframe, spark=None) -> None:
        options = {
            "driver": self._params.driver,
            "url": f"jdbc:postgresql://{self._params.host}/{self._params.database}",
            "dbtable": f"{self._params.scheme}.{self._params.table}",
            "user": self._params.user,
            "password": self._params.password,
        }
        if self._params.ssl:
            ssl_option = {
                "ssl": bool(self._params.ssl),
                "sslmode": "require",
                "sslrootcert": "/dbfs/FileStore/db_certificates/server_ca.der",
                "trustServerCertificate": True,
            }
            options.update(ssl_option)

        if self._params.mode == "overwrite":
            overwrite_options = {"truncate": self._params.clean_slate}
            options.update(overwrite_options)
        logger.info(f"Running Batch Write Query using options: {options}")
        (
            dataframe.write.mode(self._params.mode)
            .format("jdbc")
            .options(**options)
            .save()
        )
        logger.info(f"Inserted Data into {self._params.table}")

    def read_batch(self, spark):
        options = {
            "driver": self._params.driver,
            "url": f"""jdbc:postgresql://{self._params.host}/{self._params.database}""",
            "dbtable": f"""(SELECT {self._params.columns} FROM {self._params.scheme}.{self._params.table} {self._params.filter}) AS t""",
            "user": self._params.user,
            "password": self._params.password,
        }
        if self._params.ssl:
            ssl_option = {
                "ssl": bool(self._params.ssl),
                "sslmode": "require",
                "sslrootcert": "/dbfs/FileStore/db_certificates/server_ca.der",
                "trustServerCertificate": True,
            }
            options.update(ssl_option)
        logger.info(f"Running Batch Read Query using options: {options}")
        return spark.read.format("jdbc").options(**options).load()

    def run_in_db(self, stmt, fetch=False):
        """
        It takes a SQL statement and a connection object as input, and returns the result of the SQL
        statement if the fetch parameter is set to True
        :param stmt: The SQL statement to run
        :param conn_params: a class with the following attributes:
        :param fetch: if True, returns the results of the query, defaults to False (optional)
        :return: The number of rows in the table.
        """
        logger.info(f"Running statement: {stmt}")
        connection = connect(
            user=f"{self._params.user}",
            password=f"{self._params.password}",
            host=f"{self._params.host}",
            port=f"{self._params.port}",
            database=f"{self._params.database}",
        )
        cursor = connection.cursor()
        cursor.execute(stmt)
        if fetch:
            column_names = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            connection.commit()
            if connection:
                cursor.close()
                connection.close()
            return DataFrame(rows, columns=column_names)
        else:
            connection.commit()
            if connection:
                cursor.close()
                connection.close()
