from abc import ABC, abstractmethod

SERVICES = [

    'Databricks',
    'Kubernetes',
    'DBT',
    'Local'
]


class Executor(ABC):

    def __init__(self):
        pass

    @abstractmethod
    def initialize(self):
        pass

    @abstractmethod
    def load_config(self):
        pass

    @abstractmethod
    def __get_spark_session(self):
        pass