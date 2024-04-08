from datetime import datetime, timedelta
from typing import List
from vortex.params.pipeline.airflow import AirflowDatabricksTask


"""*** Methods in this module heavily rely on environment variables ***"""


def install_import_vortex():
    from pip._internal import main as pip

    pip(["uninstall", "vortex-python", "-y"])
    pip(["install", "vortex-python", "-y"])
    import vortex

    print(help(vortex))


def get_databricks_sequential_taskgroup(
    params_list, mirror_list, dag, polling_period=15, retry_limit=60
):
    from airflow.providers.databricks.operators.databricks import (
        DatabricksSubmitRunOperator,
    )
    from airflow.utils.task_group import TaskGroup

    seq_operators = []
    tg_operators = []
    for i, params_groups in enumerate(params_list):
        job_operators = []
        with TaskGroup(f"{mirror_list[i]}") as inner_tg:
            for instance_params in params_groups:
                job_params = instance_params
                adb_operator = DatabricksSubmitRunOperator(
                    task_id=f'{job_params["notebook_task"]["base_parameters"]["pipeline_name"]}',
                    run_name=f'{job_params["notebook_task"]["base_parameters"]["pipeline_name"]}',
                    json=job_params,
                    polling_period_seconds=polling_period,
                    databricks_retry_limit=retry_limit,
                    databricks_conn_id=job_params["notebook_task"]["base_parameters"][
                        "databricks_workspace"
                    ],
                    dag=dag,
                )
                job_operators.append(adb_operator)
            seq_operators.append(
                [
                    job_operators[k] >> job_operators[k + 1]
                    for k, op in enumerate(job_operators)
                    if k < len(job_operators) - 1
                ]
            )
            seq_operators
        tg_operators.append(inner_tg)
        tg_operators
    return tg_operators


def get_databricks_parallel_taskgroup(
    params_list, step_list, dag, polling_period=15, retry_limit=60
):
    from airflow.providers.databricks.operators.databricks import (
        DatabricksSubmitRunOperator,
    )
    from airflow.utils.task_group import TaskGroup

    parallel_operators = []
    tg_operators = []
    for i, params_groups in enumerate(params_list):
        job_operators = []
        with TaskGroup(f"{step_list[i]}") as inner_tg:
            for instance_params in params_groups:
                job_params = instance_params
                adb_operator = DatabricksSubmitRunOperator(
                    task_id=f'{job_params["notebook_task"]["base_parameters"]["pipeline_name"]}',
                    run_name=f'{job_params["notebook_task"]["base_parameters"]["pipeline_name"]}',
                    json=job_params,
                    polling_period_seconds=polling_period,
                    databricks_retry_limit=retry_limit,
                    databricks_conn_id=job_params["notebook_task"]["base_parameters"][
                        "databricks_workspace"
                    ],
                    dag=dag,
                )
                job_operators.append(adb_operator)
            parallel_operators.append(job_operators)
            parallel_operators
        tg_operators.append(inner_tg)
        tg_operators
    return tg_operators


def get_databricks_submit_taskgroup(taskgroup_name, instances_params, dag, sink_fork=False):
    from airflow.providers.databricks.operators.databricks import (
        DatabricksSubmitRunOperator,
    )
    from airflow.utils.task_group import TaskGroup

    with TaskGroup(taskgroup_name) as tg:
        job_operators = []
        for instance_params in instances_params:
            print(instance_params)
            if sink_fork:
                for sink in instance_params["notebook_task"]["run_parameters"][
                    "sink_kind"
                ].split(","):
                    job_params = instance_params
                    job_params["notebook_task"]["base_parameters"]["sink_kind"] = sink
                    job_operators.append(
                        DatabricksSubmitRunOperator(
                            task_id=f'submit_run_{job_params["notebook_task"]["base_parameters"]["pipeline_name"]}_{job_params["notebook_task"]["base_parameters"]["pipeline_type"]}_{sink.strip()}',
                            run_name=f'submit_run_{job_params["notebook_task"]["base_parameters"]["pipeline_name"]}_{job_params["notebook_task"]["base_parameters"]["pipeline_type"]}_{sink.strip()}',
                            json=job_params,
                            polling_period_seconds=15,
                            databricks_retry_limit=30,
                            databricks_conn_id=job_params["notebook_task"][
                                "base_parameters"
                            ]["databricks_workspace"],
                            dag=dag,
                        )
                    )
            else:
                job_params = instance_params
                job_operators.append(
                    DatabricksSubmitRunOperator(
                        task_id=f'submit_run_{job_params["notebook_task"]["base_parameters"]["pipeline_name"]}_{job_params["notebook_task"]["base_parameters"]["pipeline_type"]}',
                        run_name=f'submit_run_{job_params["notebook_task"]["base_parameters"]["pipeline_name"]}_{job_params["notebook_task"]["base_parameters"]["pipeline_type"]}',
                        json=job_params,
                        polling_period_seconds=15,
                        databricks_retry_limit=30,
                        databricks_conn_id=job_params["notebook_task"][
                            "base_parameters"
                        ]["databricks_workspace"],
                        dag=dag,
                    )
                )
    return tg


def dag_default_args(
    adb_workspace="adb_workspace",
    retries=3,
    retry_delay=30,
    owner="vortex",
    on_failure_callback=None,
    on_success_callback=None,
):
    """
    It defines the default arguments for the DAG
    :return: The default arguments for the DAG.
    """
    return {
        "owner": owner,
        "start_date": datetime(2020, 1, 1),
        "retries": retries,
        "retry_delay": timedelta(seconds=retry_delay),
        "databricks_conn_id": adb_workspace,
        "on_failure_callback": on_failure_callback,
        "on_success_callback": on_success_callback,
    }


def email_failure_notification(context, emails=""):
    from airflow.utils.email import send_email
    from os import environ

    email_to = str(environ.get("EMAIL_TO", emails)).split(",")
    task_id = context.get("task_instance").task_id
    dag_id = context.get("task_instance").dag_id
    execution_date = context.get("execution_date")
    log_url = context.get("task_instance").log_url
    try:
        send_email(
            to=email_to,
            subject=f"Vortex Orchestrator Task Failure: {task_id}",
            html_content=f"""
                            Hi, <br>
                            <p>At {execution_date}, the orchestrator failed to run task {task_id}, from the {dag_id} DAG. Check details here: </p>
                            <br> {log_url} <br>
                            """,
        )
    except Exception as e:
        print(f"Error sending email: {e}")


def email_success_notification(
    context, email_to=[]
):
    from airflow.utils.email import send_email

    task_id = context.get("task_instance").task_id
    dag_id = context.get("task_instance").dag_id
    execution_date = context.get("execution_date")
    log_url = context.get("task_instance").log_url
    try:
        send_email(
            to=email_to,
            subject=f"Vortex Orchestrator Task Success: {task_id}",
            html_content=f"""
                            Hi, <br>
                            <p>At {execution_date}, the orchestrator failed to run task {task_id}, from the {dag_id} DAG. Check details here: </p>
                            <br> {log_url} <br>
                            """,
        )
    except Exception as e:
        print(f"Error sending email: {e}")


def create_airflow_connection():
    from airflow import settings
    from airflow.models import Connection, DagModel, Variable

    try:
        conn = Connection(
            conn_id=environ["AIRFLOW_CONN_ID"],
            conn_type=environ["AIRFLOW_CONN_TYPE"],
            host=environ["AIRFLOW_CONN_HOST"],
            login=environ["AIRFLOW_CONN_LOGIN"],
            password=environ["AIRFLOW_CONN_PASSWORD"],
            description=environ["AIRFLOW_CONN_DESC"],
        )
        session = settings.Session()
        conn_name = (
            session.query(Connection).filter(Connection.conn_id == conn.conn_id).first()
        )

        if str(conn_name) == str(conn.conn_id):
            print(f"Connection {conn.conn_id} already exists")
            return 1

        else:
            session.add(conn)
            session.commit()
            print(Connection.log_info(conn))
            print(f"Connection {environ['AIRFLOW_CONN_ID']} is created")
            return conn
    except Exception as e:
        print(e)
        return 0


def unpause_dag(dag):
    """
    A way to programatically unpause a DAG.
    :param dag: DAG object
    :return: dag.is_paused is now False
    """
    from airflow import settings
    from airflow.models import Connection, DagModel, Variable

    session = settings.Session()
    try:
        qry = session.query(DagModel).filter(DagModel.dag_id == dag.dag_id)
        d = qry.first()
        d.is_paused = False
        session.commit()
    except:
        session.rollback()
    finally:
        session.close()


def write_to_az_blob(query: str, container_name, wasb_conn_id, df):
    from airflow.providers.microsoft.azure.hooks.wasb import WasbHook

    try:
        azurehook = WasbHook(wasb_conn_id=wasb_conn_id)
        azurehook.load_string(
            string_data=str(df.to_csv()),
            container_name=container_name,
            blob_name=f"{query}.csv",
        )
    except Exception as e:
        print(f"Error writing to blob: {e}")


def create_databricks_task_groups(
    dag, tasks: List[AirflowDatabricksTask], polling_period=15, retry_limit=60
):
    from airflow.providers.databricks.operators.databricks import (
        DatabricksSubmitRunOperator,
    )
    from airflow.utils.task_group import TaskGroup

    airflow_groups = {}
    airflow_tasks = {}
    for task in tasks:
        if task.task_group not in airflow_groups:
            airflow_groups[task.task_group] = TaskGroup(
                group_id=task.task_group, dag=dag
            )
        airflow_task = DatabricksSubmitRunOperator(
            task_id=task.task_id,
            task_group=airflow_groups[task.task_group],
            run_name=task.run_name,
            json=task.adb_params.dict(exclude_none=True),
            polling_period_seconds=polling_period,
            databricks_retry_limit=retry_limit,
            # databricks_conn_id = task.databricks_conn_id,
            dag=dag,
        )
        airflow_tasks[task.pipeline_name] = airflow_task
        task_dependencies = task.depends_on
        if task_dependencies:
            for dep in task_dependencies:
                dep_task = airflow_tasks[dep]
                dep_task.set_downstream(airflow_task)
    return airflow_groups


def create_dag(params, default_args):
    from airflow.models import DAG

    default_args = default_args
    dag = DAG(
        dag_id=params.manifest.name,
        default_args=default_args,
        schedule_interval=None,
        catchup=False,
        tags=[k for k in params.manifest.tags],
    )
    groups = create_databricks_task_groups(dag, params.airflow_dag_tasks_params())
    return dag
