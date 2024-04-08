from abc import ABC, abstractmethod
from pyspark.sql import DataFrame

from vortex.flows.batch.builder import PipelineBuilder
from vortex.datamodels.pipeline import RunParameters, VortexPipeline
from vortex.flows.qa import BatchQA
from vortex.flows.processing import Processing
from vortex.utils import get_class


from pyspark.sql import DataFrame, SparkSession


class PipelineBuilder(ABC):
    """
    Abstract base class for building a data pipeline.

    Attributes:
        None

    Methods:
        write(dataframe=None, spark=None): Abstract method for writing data to a destination.
        init(params): Abstract method for initializing the pipeline with parameters.
        read(spark=None): Abstract method for reading data from a source.
        process(dataframe=None, spark=None, params=None): Abstract method for processing data.
        qa(dataframe, spark=None): Abstract method for performing quality assurance on data.
    """

    @abstractmethod
    def write(self, dataframe: DataFrame = None, spark: SparkSession = None):
        """
        Abstract method for writing data to a destination.

        Args:
            dataframe (pyspark.sql.DataFrame): The data to be written.
            spark (pyspark.sql.SparkSession): The SparkSession to use for writing.

        Returns:
            None
        """

    @abstractmethod
    def init(self, params: dict):
        """
        Abstract method for initializing the pipeline with parameters.

        Args:
            params (dict): A dictionary of parameters to use for initialization.

        Returns:
            None
        """

    @abstractmethod
    def read(self, spark: SparkSession = None) -> DataFrame:
        """
        Abstract method for reading data from a source.

        Args:
            spark (pyspark.sql.SparkSession): The SparkSession to use for reading.

        Returns:
            pyspark.sql.DataFrame: The data read from the source.
        """

    @abstractmethod
    def process(
        self,
        dataframe: DataFrame = None,
        spark: SparkSession = None,
        params: dict = None,
    ) -> DataFrame:
        """
        Abstract method for processing data.

        Args:
            dataframe (pyspark.sql.DataFrame): The data to be processed.
            spark (pyspark.sql.SparkSession): The SparkSession to use for processing.
            params (dict): A dictionary of parameters to use for processing.

        Returns:
            pyspark.sql.DataFrame: The processed data.
        """

    @abstractmethod
    def qa(self, dataframe: DataFrame, spark: SparkSession = None):
        """
        Abstract method for performing quality assurance on data.

        Args:
            dataframe (pyspark.sql.DataFrame): The data to be quality assured.
            spark (pyspark.sql.SparkSession): The SparkSession to use for quality assurance.

        Returns:
            None
        """


# The `BatchETLPipelineBuilder` class is a pipeline builder for batch ETL processes, which includes
# reading data, applying processings, writing data, and running quality assurance checks.
class BatchETLPipelineBuilder(PipelineBuilder):
    """
    A class for building batch ETL pipelines.

    Inherits from PipelineBuilder.
    
    Methods:
    - init(params): Initializes the pipeline.
    - read(spark): Reads data from the source engine.
    - process(dataframe, spark, params): Processes the data.
    - write(dataframe, spark): Writes data to the sink engine.
    - qa(dataframe, spark): Runs QA on the data.
    
    Hidden Methods:
    - _set_pipeline_params(params): Sets the pipeline parameters.
    - _set_engines(params): Sets the source and sink engines.
    - _set_qa(params): Sets the BatchQA object.
    - qa_enable: Returns whether QA is enabled.
    """

    def __init__(
        self, processing: Processing = None, qa: BatchQA = None
    ) -> None:
        """
        Initializes a BatchETLPipelineBuilder object.

        Args:
        - processing (Processing): A Processing object.
        - qa (BatchQA): A BatchQA object.
        """
        self._source_engine = None
        self._sink_engine = None
        self._params = None
        self._processing = processing
        self._qa = qa

    def init(self, params: RunParameters,) -> None:
        """
        Initializes the pipeline.

        Args:
        - params (RunParameters): A RunParameters object.
        """
        self._set_pipeline_params(params)
        self._set_engines(params)
        self._set_qa(params.qa_params)

    def _set_pipeline_params(self, params: RunParameters) -> None:
        """
        Sets the pipeline parameters.

        Args:
        - params (RunParameters): A RunParameters object.
        """
        self._params = VortexPipeline(**params.dict())

    def _set_engines(self, params: RunParameters) -> None:
        """
        Sets the source and sink engines.

        Args:
        - params (RunParameters): A RunParameters object.
        """
        self._sink_engine = get_class(params.sink_engine)(params=params.sink_params)
        self._source_engine = get_class(params.source_engine)(
            params=params.source_params
        )

    def _set_qa(self, params) -> None:
        """
        Sets the BatchQA object.

        Args:
        - params: A dictionary of parameters.
        """
        self._qa = BatchQA(params)

    @property
    def qa_enable(self):
        """
        Returns whether QA is enabled.
        """
        return self._qa.qa_enable

    def read(self, spark=None):
        """
        Reads data from the source engine.

        Args:
        - spark: A SparkSession object.

        Returns:
        - A DataFrame object.
        """
        df = self._source_engine.read_batch(spark)
        df.createOrReplaceTempView("temp_view")
        return df

    def process(self, dataframe=None, spark=None, params=None) -> DataFrame:
        """
        Processes the data.

        Args:
        - dataframe: A DataFrame object.
        - spark: A SparkSession object.
        - params: A dictionary of parameters.

        Returns:
        - A DataFrame object.
        """
        params = self._params.processing_params
        return self._processing.processing(dataframe, spark, params)

    def write(self, dataframe=None, spark=None):
        """
        Writes data to the sink engine.

        Args:
        - dataframe: A DataFrame object.
        - spark: A SparkSession object.
        """
        self._sink_engine.write_batch(dataframe=dataframe, spark=spark)

    def qa(self, dataframe, spark=None):
        """
        Runs QA on the data.

        Args:
        - dataframe: A DataFrame object.
        - spark: A SparkSession object.
        """
        self._qa.run(dataframe, spark)

