from abc import ABC, abstractmethod


class Engine(ABC):
    """
    The Builder interface specifies methods for creating the different parts of
    the Product objects.
    """

    @abstractmethod
    def write_stream(self, dataframe, stage, batch_id, app_id, spark) -> None:
        pass

    @abstractmethod
    def read_stream(self, spark):
        pass

    @abstractmethod
    def write_batch(self, dataframe, spark) -> None:
        pass

    @abstractmethod
    def read_batch(self, spark):
        pass
