from pydantic import BaseModel
from vortex.datamodels.pipeline.qa import QA


class VortexPipeline(BaseModel):
    pipeline_name: str = None
    pipeline_type: str = None
    app_id: str = None
    stage: str = None
    quality: str = None
    checkpoint_path: str = None
    transformation_database: str = None
    transformation_params: dict = None
    additional_params: dict = None


class RunParameters(BaseModel):
    pipeline_name: str = None
    pipeline_type: str = None
    app_id: str = None
    stage: str = None
    quality: str = None
    checkpoint_path: str = None
    transformation_database: str = None
    additional_params: dict = None
    source_engine: str = None
    source_params: dict = None
    sink_engine: str = None
    sink_params: dict = None
    qa_params: dict = {}
    transformation_params: dict = None
    databricks_workspace: str = None
    job_id: str = None
    steps: str = None


class BaseParameters(BaseModel):
    pipeline_name: str = None
    pipeline_type: str = None
    source_kind: str = None
    sink_kind: str = None
    keyword: str = None
    search_path: str = None
    databricks_workspace: str = None
    steps: str = None


class NotebookTask(BaseModel):
    notebook_path: str = None
    run_parameters: RunParameters = None
    base_parameters: BaseParameters = None
