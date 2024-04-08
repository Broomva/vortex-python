from typing import List, Dict, Any, Optional
from pydantic import BaseModel


class DatabricksNotifications(BaseModel):
    on_start: Optional[List[str]] = []
    on_success: Optional[List[str]] = []
    on_failure: Optional[List[str]] = []
    on_duration_warning_threshold_exceeded: Optional[List[str]] = []
    no_alert_for_skipped_runs: Optional[bool] = False


class DatabricksNotificationSettings(BaseModel):
    no_alert_for_skipped_runs: Optional[bool] = False
    no_alert_for_canceled_runs: Optional[bool] = False
    alert_on_last_attempt: Optional[bool] = False


class DatabricksSchedule(BaseModel):
    quartz_cron_expression: str
    timezone_id: Optional[str] = "UTC"
    pause_status: Optional[str] = "PAUSED"


class DatabricksJobHealthRules(BaseModel):
    metric: Optional[str] = "RUN_DURATION_SECONDS"
    op: Optional[str] = "GREATER_THAN"
    value: Optional[int] = 600


class DatabricksJobHealth(BaseModel):
    rules: List[DatabricksJobHealthRules] = []


class DatabricksCluster(BaseModel):
    node_type_id: Optional[str]
    driver_node_type_id: Optional[str]
    spark_version: str
    spark_conf: Optional[dict] = {}
    spark_env_vars: Optional[dict] = {}
    init_scripts: Optional[List[dict]] = []
    data_security_mode: Optional[str] = "SINGLE_USER"
    runtime_engine: Optional[str]
    num_workers: Optional[int]
    autoscale: Optional[dict]
    aws_attributes: Optional[dict]
    azure_attributes: Optional[dict]
    custom_tags: dict
    enable_elastic_disk: Optional[bool]
    instance_pool_id: Optional[str]
    driver_instance_pool_id: Optional[str]


class DatabricksJobCluster(BaseModel):
    job_cluster_key: str
    new_cluster: DatabricksCluster


class NotebookTask(BaseModel):
    notebook_path: Optional[str]
    base_parameters: Optional[dict] = None
    source: Optional[str] = "WORKSPACE"


class DatabricksTask(BaseModel):
    task_key: str
    description: Optional[str]
    depends_on: Optional[list] = []
    job_cluster_key: Optional[str]
    new_cluster: Optional[DatabricksCluster]
    existing_cluster_id: Optional[str]
    notebook_task: Optional[NotebookTask]
    libraries: Optional[List[dict]] = []
    max_retries: Optional[int] = None
    min_retry_interval_millis: Optional[int] = None
    retry_on_timeout: Optional[bool] = None
    timeout_seconds: Optional[int] = 0
    email_notifications: Optional[DatabricksNotifications] = {}
    notification_settings: Optional[DatabricksNotificationSettings] = {}
    health: Optional[DatabricksJobHealth] = {}


class Databricksjobs(BaseModel):
    name: str
    tasks: List[DatabricksTask]
    job_clusters: List[DatabricksJobCluster]
    tags: dict
    email_notifications: Optional[DatabricksNotifications] = {}
    notification_settings: Optional[DatabricksNotificationSettings] = {}
    health: Optional[DatabricksJobHealth] = {}
    webhook_notifications: Optional[DatabricksNotifications] = {}
    schedule: Optional[DatabricksSchedule] = None
    timeout_seconds: Optional[int] = 0
    max_concurrent_runs: Optional[int] = 1
    format: Optional[str] = "MULTI_TASK"
