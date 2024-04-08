import ast
import json
from os import environ
import os
from typing import List
import importlib.util

from vortex.orchestrator.airflow import get_repo_toml_file
from vortex.params.pipeline.airflow import AirflowDatabricksTask
from vortex.params.pipeline_manifest.manifest import Manifest, ManifestBase
from vortex.params.pipeline import RunParameters
from vortex.params.pipeline_manifest.databricks import Jobs, Task
from vortex.utils.dbutils import get_dbutils
from vortex.utils import get_class
from itertools import chain
from pathlib import Path
from jinja2 import Environment, BaseLoader


class VortexManifestParser:
    """
    Class to parser the manifest for every place in Vortex
    """

    def __init__(
        self,
        spark=None,
        pipeline: str = None,
        step: str = None,
        task_group: str = None,
        mock_credentials: list = None,
        manifest: Manifest = None,
        manifest_dict: dict = None,
        library_scope: str = None,
        use_widgets: bool = True,
    ):
        self.spark = spark
        self.pipeline = pipeline
        self.step = step
        self.task_group = task_group
        self.mock_credentials = mock_credentials
        self.library_scope = library_scope
        self.manifest = Manifest(**manifest_dict) if manifest_dict else manifest
        self.manifest_dict = manifest_dict
        self.use_widgets = use_widgets

    def _merge_credentials(self, source_params: dict, credentials: dict) -> dict:
        """
        merge credentials from connections with manifest definition
        """
        # TODO: Use different dict merge strategy than for iterator since not all keys from both dicts are taken into account and debug correct merge
        updated_params = credentials.copy()
        for k in credentials.keys() & source_params.keys():
            updated_params[k] = source_params[k]
        return updated_params

    def _get_credentials(self, connection_name) -> dict:
        """
        Get credentials from db secrets
        """
        if self.spark is None:
            return json.loads(self.mock_credentials[connection_name])
        dbutils = get_dbutils(self.spark)
        params = dbutils.secrets.get(
            scope=self.manifest.library_scope, key=connection_name
        )
        return json.loads(params)

    def _set_widgets_task_params(
        self,
    ):
        # Get parameters
        if self.spark is not None and self.use_widgets:
            dbutils = get_dbutils(self.spark)
            self.pipeline = dbutils.widgets.get("pipeline")
            self.task_group = dbutils.widgets.get("task_group")
            self.step = dbutils.widgets.get("step")

    def get_manifest(self, manifest_path):
        """
        Loads the manifest from the given file.

        This method tries to load a Python module from the file specified by `manifest_name`,
        and then retrieves the manifest from the module.

        :param manifest_path:
        :return: The loaded manifest as a dictionary.
        :raises ValueError: If the manifest could not be loaded.
        """

        env = Environment(
            loader=BaseLoader(), variable_start_string="{{#", variable_end_string="#}}"
        )

        if manifest_path.suffix == ".py":
            path = manifest_path
            spec = importlib.util.spec_from_file_location("manifest", path)

            if spec is None:
                raise ValueError(f"Could not load manifest from {path}")

            module = importlib.util.module_from_spec(spec)

            if module is None:
                raise ValueError(f"Could not create module from spec {spec}")

            try:
                spec.loader.exec_module(module)
            except Exception as e:
                raise ValueError(f"Could not execute module: {e}")
            manifest = module.get_manifest(environment=os.environ.get("ENVIRONMENT"))
            if manifest is None:
                raise ValueError(f"Could not get manifest from module")
            return manifest

        elif manifest_path.suffix == ".json":
            replacements = {
                "environment": os.getenv("ENVIRONMENT"),
                "client": os.getenv("CLIENT"),
            }

            try:
                with open(manifest_path, "r") as f:
                    template_str = f.read()
                rendered_json_str = env.from_string(template_str).render(**replacements)
                data = json.loads(rendered_json_str)

                return data
            except Exception as e:
                raise ValueError(f"Could not read JSON file: {e}")

        else:
            raise ValueError(f"Unsupported file type for {manifest_path}")

    def databricks_params(self):
        """
        Gets the specific parameters of a single task
        """

        path = Path("orchestrator/manifest")
        self._set_widgets_task_params()
        FILE_EXTENSION = ["*.py", "*.json"]

        files = list(chain(*[path.glob(extension) for extension in FILE_EXTENSION]))
        manifest_name = next((file for file in files if self.pipeline in file), None)

        # Populate parameters

        if self.manifest_dict is None:
            if ".py" in manifest_name:
                manifest_dict = get_class(
                    f"orchestrator.manifest.{self.pipeline}.get_manifest"
                )(environ["ENVIRONMENT"])
            else:
                manifest_dict = self.get_manifest(manifest_name)

            self.manifest = Manifest(**manifest_dict)

        # Get the speecific task populated with the common parameters
        task_params = self.manifest.tasks[self.task_group].tasks[int(self.step) - 1]

        # Update source credentials
        source_params = self._get_credentials(
            f"engine.{task_params.source_params['kind']}.{task_params.source_params['connection_name']}"
        )
        updated_source_params = self._merge_credentials(
            task_params.source_params, source_params
        )

        # Update sink credentials
        sink_params = self._get_credentials(
            connection_name=f"engine.{task_params.sink_params['kind']}.{task_params.sink_params['connection_name']}"
        )
        updated_sink_params = self._merge_credentials(
            task_params.sink_params, sink_params
        )

        run_params = task_params.dict()
        run_params.update(
            {
                "source_engine": task_params.source_params["engine"],
                "source_params": updated_source_params,
                "sink_engine": task_params.sink_params["engine"],
                "sink_params": updated_sink_params,
            }
        )

        return RunParameters(**run_params)

    def databricks_task_params(self, task_params: ManifestBase) -> Task:
        """
        Create and parse the task params to execute inside de databricks jobs environment
        """
        task = task_params.dict()

        # Job cluster management
        if task_params.job_cluster.type == "new_cluster":
            # Loof for cluster definition in job clusters
            cluster = next(
                job_cluster
                for job_cluster in self.manifest.job_clusters
                if job_cluster.job_cluster_key == task_params.job_cluster.key
            )

            # Update task dict
            task.update({task_params.job_cluster.type: cluster.new_cluster})
        else:
            # Update task dict
            task.update({task_params.job_cluster.type: task_params.job_cluster.key})

        if task_params.task_type == "dbt":
            task.update(
                {
                    "task_key": task_params.pipeline_name,
                    "depends_on": [{"task_key": do} for do in task_params.depends_on],
                    "dbt_task": task_params.dbt_task.dict(),
                }
            )

            print(task)

        else:
            task.update(
                {
                    "task_key": task_params.pipeline_name,
                    "depends_on": [{"task_key": do} for do in task_params.depends_on],
                    "notebook_task": {
                        "notebook_path": task_params.notebook,
                        "base_parameters": {
                            "pipeline": self.manifest.name,
                            "task_group": task_params.task_group,
                            "step": task_params.step,
                            "library_scope": self.manifest.library_scope,
                        },
                    },
                }
            )

        return Task(**task)

    def databricks_workflows_params(self) -> Jobs:
        """
        Get databricks params structure to send like a json using de workflows api.
        """

        tasks = []
        for task_group_name, task_group in self.manifest.tasks.items():
            if task_group.enable is True:
                for raw_task in task_group.tasks:
                    task = self.databricks_task_params(raw_task)
                    tasks.append(task)

        manifest_dict = self.manifest.dict()
        manifest_dict["tasks"] = tasks

        return Jobs(**manifest_dict)

    def airflow_dag_tasks_params(self) -> List[AirflowDatabricksTask]:
        """
        Get airflows params structure to create a dag inside airflos.
        """
        tasks = []
        for task_group_name, task_group in self.manifest.tasks.items():
            if task_group.enable is True:
                for raw_task in task_group.tasks:
                    adb_task = self.databricks_task_params(raw_task)
                    task = raw_task.dict()
                    task.update({"adb_params": adb_task.dict()})
                    tasks.append(AirflowDatabricksTask(**task))
        return tasks


def parser_retrieve(keyword, search_path):
    params_list = list(get_repo_toml_file(keyword, search_path))

    adb_params = [
        adb_param[list(adb_param.keys())[0]]["notebook_params"]
        for adb_param in params_list
    ]

    print(f"Loading parser: {adb_params}")
    return [
        (adb_params[i]["cluster_params_parser"], adb_params[i]["job_params_parser"])
        for i, val in enumerate(adb_params)
    ][0]
