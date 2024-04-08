from os import environ
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, root_validator

from vortex.datamodels.pipeline.workflows import (
    DatabricksJobCluster,
    DatabricksNotifications,
    DatabricksSchedule,
    DatabricksJobHealth,
    DatabricksNotificationSettings,
)


class ManifestTransformationParams(BaseModel):
    logic_type: str = None
    business_logic: str = None
    business_logic_params: Optional[dict] = {}


class ManifestTaskJobParams(BaseModel):
    type: str = None
    key: str = None


class ManifestTask(BaseModel):
    pipeline_name: Optional[str]
    pipeline_type: Optional[str]
    task_group: Optional[str]
    step: Optional[int] = None
    app_id: Optional[str] = None
    stage: Optional[str] = None
    quality: Optional[str] = None
    steps: Optional[str] = None
    checkpoint_base_path: Optional[str] = "dbfs:/vortex_checkpoints"
    deployment_version: Optional[str] = ""
    checkpoint_path: Optional[str] = None
    additional_params: Optional[dict] = {}
    depends_on: Optional[List[int]] = []
    notebook: Optional[str]
    job_cluster: Optional[ManifestTaskJobParams]
    max_retries: Optional[int] = None
    min_retry_interval_millis: Optional[int] = None
    retry_on_timeout: Optional[bool] = None
    timeout_seconds: Optional[int] = 0
    libraries: Optional[List[dict]]
    source_params: Optional[dict]
    sink_params: Optional[dict]
    transformation_params: Optional[ManifestTransformationParams]
    qa_params: Optional[dict] = {}
    databricks_conn_id: Optional[str] = None
    email_notifications: Optional[DatabricksNotifications] = {}
    notification_settings: Optional[DatabricksNotificationSettings] = {}
    health: Optional[DatabricksJobHealth] = {}


class ManifestTaskGroup(BaseModel):
    depends_on: Optional[List[str]] = []
    enable: Optional[bool] = True
    tasks: List[ManifestTask]


class Manifest(BaseModel):
    name: str
    tags: dict
    library_scope: str
    email_notifications: Optional[DatabricksNotifications] = {}
    notification_settings: Optional[DatabricksNotificationSettings] = {}
    health: Optional[DatabricksJobHealth] = {}
    schedule: Optional[DatabricksSchedule] = None
    job_clusters: Optional[List[DatabricksJobCluster]] = []
    tasks: Dict[str, ManifestTaskGroup]
    default_task: dict

    @root_validator(pre=True)
    def populate_task_default(cls, values):
        tasks = values["tasks"]
        default_task = values["default_task"]
        for task_group_name, task_group in tasks.items():
            for index, raw_task in enumerate(task_group["tasks"]):
                task = ManifestTask(**{**default_task, **raw_task})
                task.step = str(index + 1).zfill(3)
                source_kind = task.source_params["kind"]
                sink_kind = task.sink_params["kind"]
                task.pipeline_name = f"{environ['ENVIRONMENT']}_{task.sink_params['table']}_{task_group_name}_{task.pipeline_type}_{task.step}_{source_kind[:1]}t{sink_kind[:1]}"
                task.steps = f"{source_kind}_to_{sink_kind}"
                task.checkpoint_path = f"{task.checkpoint_base_path}/{task.pipeline_name}_checkpoint_{task.deployment_version}"
                task.app_id = f"{task.pipeline_name}_app_id_{task.deployment_version}"
                task.task_group = task_group_name
                task_group["tasks"][index] = task

        for task_group_name, task_group in tasks.items():
            # tasks dependency
            for task in task_group["tasks"]:
                task.depends_on = [
                    task_group["tasks"][i - 1].pipeline_name for i in task.depends_on
                ]
            # task group dependency
            task_group["tasks"][0].depends_on = [
                tasks[i]["tasks"][-1].pipeline_name for i in task_group["depends_on"]
            ]

        return values
