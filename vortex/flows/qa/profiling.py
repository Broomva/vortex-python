from pydeequ.profiles import *
import pandas as pd
from vortex.datamodels.pipeline.qa import Profiling
from vortex.engine.delta import DeltaEngine
from pyspark.sql.functions import *


class TableProfiling:
    def __init__(self, params):
        self._params = Profiling(**params)

        self._sink = self._set_sink()

    def _set_sink(self):
        engine_params = {
            "database": self._params.qa_database,
            "table": self.get_profiling_table,
            "mode": "append",
        }

        return DeltaEngine(engine_params)

    @property
    def get_profiling_table(self):
        return f"{self._params.source_table}_profiling"

    def profiling(self, dataframe, spark):
        from pyspark.sql.types import StructType, StructField, StringType, DoubleType

        result_schema = StructType(
            [
                StructField("column", StringType(), True),
                StructField("completeness", DoubleType(), True),
                StructField("approximateNumDistinctValues", DoubleType(), True),
                StructField("datatype", StringType(), True),
                StructField("booleans", DoubleType(), True),
                StructField("Fractional", DoubleType(), True),
                StructField("Integral", DoubleType(), True),
                StructField("Unknown", DoubleType(), True),
                StructField("String", DoubleType(), True),
                StructField("value", StringType(), True),
                StructField("count", DoubleType(), True),
                StructField("ratio", DoubleType(), True),
            ]
        )

        if dataframe.count() == 0:
            return spark.createDataFrame([], result_schema)

        result = ColumnProfilerRunner(spark).onData(dataframe).run()

        results_list = list()
        for col, profile in result.profiles.items():
            result_dict = {
                "column": profile.column,
                "completeness": profile.completeness,
                "approximateNumDistinctValues": profile.approximateNumDistinctValues,
                "datatype": profile.dataType,
                "booleans": profile.typeCounts.get("Boolean"),
                "Fractional": profile.typeCounts.get("Fractional"),
                "Integral": profile.typeCounts.get("Integral"),
                "Unknown": profile.typeCounts.get("Unknown"),
                "String": profile.typeCounts.get("String"),
                "histogram": profile.histogram,
            }

            results_list.append(result_dict)

        result_df = pd.DataFrame.from_records(results_list)

        mask = result_df.applymap(lambda x: x is None)
        cols = result_df.columns[(mask).any()]
        for col in result_df[cols]:
            result_df.loc[mask[col], col] = ""

        histogram = result_df[["column", "histogram"]]
        histogram_list = []
        histogram_columns = ["column", "value", "count", "ratio"]
        for idx, row in histogram[(histogram.histogram != None)].iterrows():
            for value in row["histogram"]:
                tmp = {
                    "column": row["column"],
                    "value": value.value,
                    "count": value.count,
                    "ratio": value.ratio,
                }

                histogram_list.append(tmp)

        histogram_df = pd.DataFrame.from_records(
            histogram_list, columns=histogram_columns
        )

        analysis_final = result_df[
            [
                "column",
                "completeness",
                "approximateNumDistinctValues",
                "datatype",
                "booleans",
                "Fractional",
                "Integral",
                "Unknown",
                "String",
            ]
        ].merge(histogram_df, how="outer", on="column")

        return spark.createDataFrame(analysis_final)

    def _get_table(self, spark, version):
        query = f"select * from {self._params.source_database}.{self._params.source_table} VERSION AS OF {version}"
        print(query)
        return spark.sql(query)

    def _save_results(self, dataframe_result, spark):
        self._sink.write_batch(dataframe_result, spark)

    def _get_Last_table_version(self, spark, is_source_table):
        database = (
            self._params.source_database
            if is_source_table
            else self._params.qa_database
        )
        table = (
            self._params.source_table if is_source_table else self.get_profiling_table
        )

        metadata = (
            spark.sql(f"DESCRIBE HISTORY {database}.{table}")
            .orderBy(col("version").desc())
            .limit(1)
            .collect()[0]
            .asDict()
        )

        return metadata["version"], metadata["timestamp"]

    def _get_first_table_version(self, spark):
        database = self._params.source_database
        table = self._params.source_table

        version = (
            spark.sql(f"DESCRIBE HISTORY {database}.{table}")
            .orderBy(col("version").asc())
            .limit(1)
            .collect()[0][0]
        )

        return version

    def _get_Last_table_version_profiled(self, spark):
        database = self._params.qa_database
        table = self.get_profiling_table

        version = spark.sql(
            f"select version from {database}.{table} order by version limit 1"
        ).collect()[0][0]

        return version

    def run(self, spark):
        end, timestamp = self._get_Last_table_version(spark, True)
        print(end, timestamp)
        if spark.catalog.tableExists(
            f"{self._params.qa_database}.{self.get_profiling_table}"
        ):
            start = self._get_Last_table_version_profiled(spark)
        else:
            start = self._get_first_table_version(spark)

        if int(end) - int(start) > 5:
            start = end - 5

        print(start, end)

        for table_version in range(start, end + 1):
            print(f"{self._params.source_table}", "version:", table_version, "of:", end)

            dataframe = self._get_table(spark, table_version)

            dataframe_result = (
                self.profiling(dataframe, spark)
                .withColumn("version", lit(table_version))
                .withColumn("timestamp", lit(timestamp))
                .withColumn("version_row_count", lit(dataframe.count()))
            )

            self._save_results(dataframe_result, spark)
