# import pytest
# from pydantic import ValidationError

# from vortex.params.pipeline_manifest.manifest import (Manifest, ManifestTask,
#                                                    ManifestTaskGroup,
#                                                    Notifications, Schedule)


# @pytest.fixture
# def sample_notification():
#     return {"on_start": ["start@mail.com"]}


# @pytest.fixture
# def sample_schedule():
#     return {"quartz_cron_expression": "* * * * *", "timezone_id": "UTC", "pause_status": "PAUSED"}


# @pytest.fixture
# def sample_cluster():
#     return {}


# @pytest.fixture
# def sample_job_cluster(sample_cluster):
#     return {"job_cluster_key": "test_key", "new_cluster": sample_cluster}


# @pytest.fixture
# def sample_transformation_params():
#     return {"logic_type": "type1", "business_logic": "logic1", "business_logic_params": {"param1": "value1"}}


# @pytest.fixture
# def sample_manifest_task(sample_job_cluster, sample_transformation_params):
#     return {
#         "notebook": "notebook_path",
#         "job_cluster": sample_job_cluster,
#         "transformation_params": sample_transformation_params,
#         "source_params": {"kind": "source"},
#         "sink_params": {"kind": "sink", "table": "table_name"},
#         "pipeline_type": "type1"
#     }


# @pytest.fixture
# def sample_manifest_task_group(sample_manifest_task):
#     return {"enable": True, "tasks": [sample_manifest_task]}


# @pytest.fixture
# def sample_manifest_input(sample_notification, sample_schedule, sample_manifest_task_group):
#     # Add two more tasks to each task group
#     additional_tasks = [sample_manifest_task_group["tasks"][0] for _ in range(2)]
#     sample_manifest_task_group["tasks"].extend(additional_tasks)

#     additional_task_group = sample_manifest_task_group.copy()
#     return {
#         "name": "test_manifest",
#         "tags": {"tag1": "value1"},
#         "library_scope": "test_scope",
#         "email_notifications": [Notifications(**sample_notification)],
#         "schedule": Schedule(**sample_schedule),
#         "job_clusters": [],
#         "tasks": {
#             "group1": ManifestTaskGroup(**sample_manifest_task_group),
#             "group2": ManifestTaskGroup(**additional_task_group)
#         },
#         "default_task": {}
#     }

# @pytest.fixture
# def expected_manifest_output():
#     return {
#         "name": "example_manifest",
#         "tags": {"tag1": "value1"},
#         "library_scope": "test_scope",
#         "email_notifications": [
#             {
#                 "on_start": ["start@mail.com"],
#                 "on_success": [],
#                 "on_failure": [],
#                 "no_alert_for_skipped_runs": False
#             }
#         ],
#         "schedule": {
#             "quartz_cron_expression": "0 0 * * * ?",
#             "timezone_id": "UTC",
#             "pause_status": "PAUSED"
#         },
#         "job_clusters": [],
#         "tasks": {
#             "task_group_1": {
#                 "depends_on": [],
#                 "enable": True,
#                 "tasks": [
#                 ]
#             },
#             "task_group_2": {
#                 "depends_on": [],
#                 "enable": True,
#                 "tasks": [
#                 ]
#             }
#         },
#         "default_task": {},
#     }





# def test_schedule_model(sample_schedule):
#     schedule = Schedule(**sample_schedule)
#     assert schedule.quartz_cron_expression == "* * * * *"
#     assert schedule.timezone_id == "UTC"


# def test_manifest_task_model(sample_manifest_task):
#     manifest_task = ManifestTask(**sample_manifest_task)
#     assert manifest_task.notebook == "notebook_path"
#     assert manifest_task.source_params["kind"] == "source"


# def test_manifest_task_group_model(sample_manifest_task_group):
#     manifest_task_group = ManifestTaskGroup(**sample_manifest_task_group)
#     assert manifest_task_group.enable is True
#     assert manifest_task_group.tasks[0].notebook == "notebook_path"


# def test_manifest_model(sample_manifest_input, monkeypatch):
#     monkeypatch.setenv("ENVIRONMENT", "test_env")

#     manifest = Manifest(**sample_manifest_input)

#     for group_name, task_group in manifest.tasks.items():
#         assert len(task_group.tasks) == 3  # Check if there are 3 tasks in each task group
#         for task in task_group.tasks:
#             assert task.pipeline_name.startswith(f"test_env_table_name_{group_name}")
#             assert task.steps == "source_to_sink"
#             assert task.checkpoint_path.startswith(f"dbfs:/vortex_checkpoints/test_env_table_name_{group_name}")
#             assert task.app_id.startswith(f"test_env_table_name_{group_name}_app_id")
#             assert task.task_group == group_name