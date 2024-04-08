def vortex_pipeline_template():
    return """{
            "pipeline_params": {
                "pipeline_name": "{{ pipeline_name }}",
                "pipeline_type": "{{ pipeline_type }}",
                "app_id": "{{ app_id }}",
                "stage":"{{ stage }}",
                "quality": "{{ quality }}",
                "checkpoint_path": "{{ checkpoint_path }}",
                "transformation_database": "{{ transformation_database }}",
                "additional_params": "{{ additional_params }}"
            },
            "source_params": {
                "engine": "{{ source_engine }}",
                "engine_params": "{{ source_params }}"
            },
            "sink_params": {
                "engine": "{{ sink_engine }}",
                "engine_params": "{{ sink_params }}"
            },
            "qa_params": "{{ qa_params }}"
        }"""


def vortex_pipeline_template_azdevops():
    return """{
            "pipeline_params": {
                "pipeline_name": "{{ name }}",
                "pipeline_type": "{{ type }}",
                "app_id": "{{ app_id }}",
                "stage":"{{ stage }}",
                "quality": "{{ quality }}",
                "checkpoint_path": "{{ checkpoint_path }}",
                "transformation_database": "{{ transformation_database }}",
                "additional_params": "{{ additional_params }}"
            },
            "source_params": {
                "engine": "{{ source_engine }}",
                "engine_params": "{{ source_params }}"
            },
            "sink_params": {
                "engine": "{{ sink_engine }}",
                "engine_params": "{{ sink_params }}"
            },
            "qa_params": "{{ qa_params }}"
        }"""


def parser_vortex_pipeline_params_template_azdevops():
    return """{
              "title": "{{ title }}",
              "owner": {
                "name": "{{ owner_name }}"
             },
              "job": {
                "task_id": "{{ job_task_id }}",
                "job_name": "{{ job_job_name }}",
                "job_id": "{{ job_job_id }}",
                "cluster_name": "{{ job_cluster_name }}",
                "spark_version": "{{ job_spark_version }}",
                "databricks_workspace": "{{ job_databricks_workspace }}",
                "cluster_log_conf_path": "{{ job_cluster_log_conf_path }}",
                "init_scripts_path": "{{ job_init_scripts_path }}",
                "notebook_path": "{{ job_notebook_path }}",
                "keyword": "{{ job_keyword }}",
                "search_path": "{{ job_search_path }}",
                "existing_cluster_id": "{{ job_existing_cluster_id }}",
                "autoscale": {
                  "autoscale_flag": "{{ job_autoscale_autoscale_flag }}",
                  "min_workers": "{{ job_autoscale_min_workers  }}",
                  "max_workers": "{{ job_autoscale_max_workers }}"
               },
                "standard_cluster": {
                  "num_workers": "{{ job_standard_cluster_num_workers }}",
                  "node_type_id": "{{ job_standard_cluster_node_type_id }}",
                  "driver_node_type_id": "{{ job_standard_cluster_driver_node_type_id }}"
               },
                "pool_cluster": {
                  "pool_cluster_flag": "{{ job_pool_cluster_pool_cluster_flag }}",
                  "instance_pool_id": "{{ job_pool_cluster_instance_pool_id }}",
                  "driver_instance_pool_id": "{{ job_pool_cluster_driver_instance_pool_id }}",
                  "runtime_engine": "{{ job_pool_cluster_runtime_engine }}"
               },
                "env_vars": {
                  "datadog_api_key": "{{ job_env_vars_datadog_api_key }}",
                  "datadog_environment": "{{ job_env_vars_datadog_environment }}",
                  "artifact_path": "{{ job_env_vars_artifact_path }}",
                  "repo_path": "{{ job_env_vars_repo_path }}",
                  "GIT_USER": "{{ job_env_vars_GIT_USER }}",
                  "GIT_PAT": "{{ job_env_vars_GIT_PAT }}",
                  "GIT_ORG": "{{ job_env_vars_GIT_ORG }}",
                  "GIT_PROJECT_ID": "{{ job_env_vars_GIT_PROJECT_ID }}",
                  "GIT_REPO_ID": "{{ job_env_vars_GIT_REPO_ID }}",
                  "GIT_BRANCH": "{{ job_env_vars_GIT_BRANCH }}"
               }
             },
              "pipeline": {
                "pipeline_name": "{{ pipeline_pipeline_name }}",
                "pipeline_type": "{{ pipeline_pipeline_type }}",
                "app_id": "{{ pipeline_app_id }}",
                "stage": "{{ pipeline_stage }}",
                "quality": "{{ pipeline_quality }}",
                "checkpoint_path": "{{ pipeline_checkpoint_path }}",
                "transformation_database": "{{ pipeline_transformation_database }}",
                "steps": "{{ pipeline_steps }}"
             },
              "engine": {
                "cores_count": "{{ additional_cores_count }}",
                "mode": "{{ additional_mode }}",
                "partitioning_factor": "{{ additional_partitioning_factor }}",
                "paralellism_factor": "{{ additional_paralellism_factor }}",
                "throttling_enabled": "{{ additional_throttling_enabled }}"
             },
              "source": {
                "kind": "{{ source_kind }}",
                "engine": "{{ source_engine }}"
             },
              "sink": {
                "kind": "{{ sink_kind }}",
                "engine": "{{ sink_engine }}"
             },
              "processing": {
                "payload_schema": "{{ additional_payload_schema }}",
                "business_logic": "{{ additional_business_logic }}",
                "logic_type": "{{ additional_logic_type }}",
                "validation": {
                  "qa_flag": "{{ additional_validation_qa_flag }}",
                  "qa_jar_path": "{{ additional_validation_qa_jar_path }}"
               },
                "inference": {
                  "fade_flag": "{{ additional_inference_fade_flag }}",
                  "fade_table": "{{ additional_inference_fade_table }}"
               }
             }
            }"""


def qa_pipeline_template():
    return """{
              "checks": "{{ checks }}",
              "raise_alert": "{{ raise_alert }}",
              "qa_db": "{{ qa_db }}",
              "qa_table": "{{ qa_table }}",
              "qa_enable": "{{ qa_enable }}",
              "check_name": "{{ check_name }}"
            }"""
