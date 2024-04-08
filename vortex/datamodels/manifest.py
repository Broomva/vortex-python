from pydantic import BaseModel, ValidationError, validator, root_validator

from vortex.executor import SERVICES
from vortex.datamodels.executor.databricks.cluster import DatabricksCluster
from vortex.datamodels.executor.kubernetes.cluster import KubernetesCluster
from vortex.datamodels.pipeline.ETL import Pipeline as ETL_pipeline
from vortex.datamodels.pipeline.DBT import Pipeline as DBT_pipeline
from vortex.datamodels.pipeline.QA import Pipeline as QA_pipeline
from vortex.datamodels.orchestrator.databricks import Orchestration as Databricks_Orchestration
from vortex.datamodels.orchestrator.airflow import Orchestration as Airflow_Orchestration
from vortex.datamodels.orchestrator.celery import Orchestration as Celery_Orchestration


class MainModel(BaseModel):
    executor: str
    cluster: Optional[Union[DatabricksCluster, KubernetesCluster]]
    pipeline: Optional[List[Union[ETL_pipeline, DBT_pipeline, QA_pipeline]]]
    pipeline_type: Literal["QA", "ETL", "DBT"]
    orchestration_type: Literal["Databricks", "Airflow", "Celery"]
    orchestration: Optional[
        Union[Databricks_Orchestration, Airflow_Orchestration, Celery_Orchestration]
    ]

    @root_validator
    def validate_cluster_based_on_executor(cls, values):
        executor = values.get("executor")
        cluster = values.get("cluster")

        EXECUTOR_CLUSTER_MAP = {
            "Databricks": DatabricksCluster,
            "Kubernetes": KubernetesCluster,
        }

        cluster_class = EXECUTOR_CLUSTER_MAP.get(executor)
        if cluster_class and not isinstance(cluster, cluster_class):
            raise ValueError(
                f"For executor '{executor}', cluster should be of type {cluster_class.__name__}"
            )

        return values

    @root_validator
    def validate_pipeline_based_on_pipeline_type(cls, values):
        pipeline_type = values.get("pipeline_type")
        pipeline = values.get("pipeline")

        PIPELINE_TYPE_MAP = {
            "QA": QA_pipeline,
            "ETL": ETL_pipeline,
            "DBT": DBT_pipeline,
        }

        pipeline_class = PIPELINE_TYPE_MAP.get(pipeline_type)
        if pipeline and any(not isinstance(p, pipeline_class) for p in pipeline):
            raise ValueError(
                f"For pipeline_type '{pipeline_type}', all pipeline items should be of type {pipeline_class.__name__}"
            )

        return values

    @root_validator
    def validate_orchestration_based_on_orchestration_type(cls, values):
        orchestration_type = values.get("orchestration_type")
        orchestration = values.get("orchestration")

        ORCHESTRATION_TYPE_MAP = {
            "Databricks": Databricks_Orchestration,
            "Airflow": Airflow_Orchestration,
            "Celery": Celery_Orchestration,
        }

        orchestration_class = ORCHESTRATION_TYPE_MAP.get(orchestration_type)
        if orchestration and not isinstance(orchestration, orchestration_class):
            raise ValueError(
                f"For orchestration_type '{orchestration_type}', orchestration should be of type {orchestration_class.__name__}"
            )

        return values

    @validator("executor", pre=True, always=True)
    def validate_executor(cls, value):
        if value not in SERVICES:
            raise ValueError(f'Executor "{value}" is not a valid service: {SERVICES}')
        return value