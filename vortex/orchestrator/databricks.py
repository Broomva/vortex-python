import json
import os

from vortex.io.requests import run_post_request
from vortex.orchestrator.parser import VortexManifestParser
from vortex.utils import get_class


class Databricks:
    """
    Databricks standard class
    """

    def __init__(
        self,
        environment: str = "dev",
        domain: str = "",
        token: str = "",
        client: str = "",
    ):
        self.domain = domain
        self.client = client
        self.environment = environment
        self.headers = {"Authorization": f"Bearer {token}"}


class DatabricksWorkflows(Databricks):
    """
    This class allow create workflows in the databricks workspace
    """

    def __init__(
        self,
        environment: str = "dev",
        domain: str = "",
        token: str = "",
        path: str = "",
        client: str = "",
    ):
        super(DatabricksWorkflows, self).__init__(environment, domain, token, client)
        self.__path = path
        self.__env = environment

    def _read_exisiting_jobs(self) -> dict:
        try:
            f = open(f"{self.__path}/current_jobs.json")
            jobs = json.loads(f.read())
        except FileNotFoundError:
            jobs = {}
        return jobs

    def write_jobs(self) -> None:
        dir_list = [d for d in os.listdir(self.__path) if ".py" in d]
        for d in dir_list:
            m = get_class(f"{self.__path.replace('/', '.')}.{d[:-3]}.get_manifest")
            manifest = m(self.__env)

            jobs = VortexManifestParser(
                manifest_dict=manifest
            ).databricks_workflows_params()
            existing_jobs = self._read_exisiting_jobs()
            print(f"Creating/Updating workflow: {jobs.name}")
            if jobs.name in existing_jobs:
                data = {"job_id": existing_jobs[jobs.name], "new_settings": jobs.dict()}
                url = f"{self.domain}/api/2.1/jobs/reset"
                response = run_post_request(
                    json.dumps(data, indent=4), url, self.headers
                )
                if response.status_code != 200:
                    raise Exception(f" Error creating workflow: {response.text}")
            else:
                data = jobs.dict()
                url = f"{self.domain}/api/2.1/jobs/create"
                response = run_post_request(
                    json.dumps(data, indent=4), url, self.headers
                )
                if response.status_code != 200:
                    raise Exception(f" Error creating workflow: {response.text}")

                existing_jobs[jobs.name] = response.json()["job_id"]
                json_object = json.dumps(existing_jobs, indent=4)
                with open(f"{self.__path}/current_jobs.json", "w") as outfile:
                    outfile.write(json_object)
        return None


class DatabricksSecrets(Databricks):
    """
    This class allow write or update databricks screts
    """

    def __init__(
        self,
        client: str = "",
        environment: str = "dev",
        domain: str = "",
        token: str = "",
        credentials: list = [],
    ):
        super(DatabricksSecrets, self).__init__(environment, domain, token, client)
        self.__credentials = credentials

    def create_scope(self, non_premium=False) -> None:
        url = f"{self.domain}/api/2.0/secrets/scopes/create"
        data = {"scope": f"{self.environment}_{self.client}_connections"}
        if non_premium:
            data["initial_manage_principal"] = "users"

        return run_post_request(json.dumps(data, indent=4), url, self.headers)

    def write_secret(self):
        response = self.create_scope()
        if (
            response.status_code != 200
            and response.json()["error_code"] != "RESOURCE_ALREADY_EXISTS"
            and response.json()["message"]
            != 'Premium Tier is disabled in this workspace. Secret scopes can only be created with initial_manage_principal "users".'
        ):
            raise Exception(f" Error creating scope: {response.text}")
        if (
            response.json()["message"]
            == 'Premium Tier is disabled in this workspace. Secret scopes can only be created with initial_manage_principal "users".'
        ):
            response = self.create_scope(non_premium=True)
        url = f"{self.domain}/api/2.0/secrets/put"
        for key in self.__credentials:
            data = {
                "scope": f"{self.environment}_{self.client}_connections",
                "key": key,
                "string_value": self.__credentials[key],
            }
            response = run_post_request(json.dumps(data, indent=4), url, self.headers)
            if response.status_code != 200:
                raise Exception(f" Error creating secret: {response.text}")
        return
