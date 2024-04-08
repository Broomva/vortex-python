from vortex.flows.batch.builder import PipelineBuilder

class BatchETLPipelineDirector:
    def __init__(self) -> None:
        self._builder = None

    @property
    def builder(self) -> PipelineBuilder:
        return self._builder

    @builder.setter
    def builder(self, builder: PipelineBuilder) -> None:
        self._builder = builder

    def _qa_gate(self, dataframe, spark):
        if self._builder.qa_enable:
            self._builder.qa(dataframe, spark)

    def etl(self, spark):
        try:
            dataframe = self._builder.read(spark)
            dataframe = self._builder.process(dataframe, spark)
            self._qa_gate(dataframe, spark)
            self._builder.write(dataframe=dataframe, spark=spark)
            return dataframe
        except ValueError as e:
            print(f"Error in job: {e}")
