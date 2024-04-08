import pandas as pd


class Files:
    @staticmethod
    def csv(dataframe=None, spark=None, mode=None, params=None, bytes=None):
        if mode == "out":
            if isinstance(dataframe, pd.DataFrame):
                return dataframe.to_csv(index=False, sep=params.separator)
            else:
                return dataframe.toPandas().to_csv(index=False, sep=params.separator)

        if mode == "in":
            name = [params.path, f"""{params.name}.{params.type}"""]
            if not spark:
                return pd.read_csv(
                    filepath_or_buffer="/".join(name), sep=params.separator
                )

            if spark:
                if bytes:
                    return spark.createDataFrame(pd.read_csv(bytes))

                return spark.read.csv("/".join(name), sep=params.separator)

    def json(self, dataframe, spark):
        pass

    def excel(self, dataframe, spark):
        pass

    def _excel_multi_page(self, dataframe, spark):
        pass
