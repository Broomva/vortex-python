from typing import Optional, List
from pydantic import BaseModel, root_validator
from vortex.datamodels.pipeline.workflows import DatabricksTask


class AirflowDatabricksTask(BaseModel):
    pipeline_name: str
    task_group: Optional[str]
    task_id: Optional[str]
    run_name: Optional[str]
    adb_params: DatabricksTask
    depends_on: Optional[List[str]] = []
    polling_period_seconds: Optional[int] = 15
    databricks_retry_limit: Optional[int] = 60
    databricks_conn_id: Optional[str] = "adb_workspace"

    @root_validator
    def set_total(cls, values):
        pipeline_name = values["pipeline_name"]
        values["task_id"] = pipeline_name
        values["run_name"] = pipeline_name
        return values
