from typing import Optional, List
from arcan.model.cluster import Cluster


class DatabricksCluster(Cluster):
    data_security_mode: Optional[str] = "SINGLE_USER"
    runtime_engine: Optional[str]
    num_workers: Optional[int]
    autoscale: Optional[dict]
    attributes: Optional[dict]
    custom_tags: dict
    enable_elastic_disk: Optional[bool]
    instance_pool_id: Optional[str]
    driver_instance_pool_id: Optional[str]
    node_type_id: Optional[str]