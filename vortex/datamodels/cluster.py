from pydantic import BaseModel
from typing import Optional, List


class Cluster(BaseModel):
    provider: str
    id: Optional[str]
    driver_node_type_id: Optional[str]
    spark_version: str
    spark_conf: Optional[dict]
    spark_env_vars: Optional[dict]
    init_scripts: Optional[List[dict]]