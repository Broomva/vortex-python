import json
import os
import pathlib
from json import load, loads
from math import floor

from vortex.datamodels.pipeline.template import (qa_pipeline_template,
                                          vortex_pipeline_template,
                                          vortex_pipeline_template_azdevops)


def get_iter_progress(iter_list, current_iteration, progress_bar):
    progress_fraction = (
        float(len(iter_list)) - float((len(iter_list) - current_iteration))
    ) / float(len(iter_list))
    progress_bar = f"={progress_bar * floor(progress_fraction * 100)}>"
    return progress_fraction, progress_bar


def get_class(kls):
    parts = kls.split(".")
    module = ".".join(parts[:-1])
    m = __import__(module)
    for comp in parts[1:]:
        m = getattr(m, comp)
    return m


def to_json_object(json):
    return json if isinstance(json, dict) else loads(json)


def get_parameters_template(template_path):
    return pathlib.Path(template_path).read_text()


def fill_json_template_from_path(
    template_path: str = None,
    values: dict = None,
    load_from_file: bool = False,
    source: str = "toml",
):
    from json_templates import JsonTemplates

    json_tmp = JsonTemplates()
    if load_from_file:
        template = get_parameters_template(template_path)
    elif source == "toml":
        template = vortex_pipeline_template()
    else:
        template = vortex_pipeline_template_azdevops()

    json_tmp.loads(template)
    return json_tmp.generate(values)


def fill_json_template_from_string(template: str, values: dict):
    from json_templates import JsonTemplates

    json_tmp = JsonTemplates()
    json_tmp.loads(template)

    return json_tmp.generate(values)


def get_function(class_obj, fun_name: str):
    try:
        return getattr(class_obj, fun_name)
    except ValueError:
        return None


def set_eventhub_offsets(
    offsets: list,
    eventhub_name,
):
    ehName = eventhub_name
    positionKey = {"ehName": ehName, "partitionId": 0}
    eventPosition = {
        "offset": "@latest",
        "seqNo": -1,
        "enqueuedTime": None,
        "isInclusive": True,
    }
    positionMap = {}
    for i, val in enumerate(offsets):
        position_key = positionKey.copy()
        position_key["partitionId"] = i

        event_position = eventPosition.copy()
        event_position["offset"] = val

        positionMap.update({json.dumps(position_key): event_position})
    return positionMap


#     if start_offsets:
#   conf["eventhubs.startingPositions"] = json.dumps(set_eventhub_offsets(offsets = start_offsets , eventhub_name = eventhub_name))

# if end_offsets:
#   conf["eventhubs.endingPositions"] = json.dumps(set_eventhub_offsets(offsets = end_offsets , eventhub_name = eventhub_name))


def get_json_schema_of_dataframe_column(
    dataframe, json_column_name: str = "body"
) -> str:
    from pyspark.sql.functions import schema_of_json

    body_df = (
        dataframe.withColumn("body", dataframe[json_column_name].cast("string"))
        .select(json_column_name)
        .limit(1)
    )
    schema_df = body_df.withColumn(
        "schema", schema_of_json(body_df.select("body").head()[0])
    )
    return schema_df.select("schema").collect()[0][0]


def fill_qa_json_template(values):
    from json_templates import JsonTemplates

    json_tmp = JsonTemplates()
    template = qa_pipeline_template()
    json_tmp.loads(template)

    return json_tmp.generate(values)


def create_func_from_string(func_code_str):
    g = {}
    l = {}
    exec(func_code_str, g, l)
    if l:
        return list(l.values())[0]


def call_methods_from_object(module, function_name):
    return getattr(module, function_name)

def f7(seq):
    seen = set()
    seen_add = seen.add
    return [x for x in seq if not (x in seen or seen_add(x))]


def get_file_path(file_name):
    found_file = False
    for root, dirs, files in os.walk("."):
        for filename in files:
            if filename == file_name:
                found_file = True
                print(f"Found file {file_name} at path {os.path.join(root, filename)}")
                return os.path.join(root, filename)
    if not found_file:
        print(f"Could not find file {file_name} in the local filesystem")


def get_workflows_job_id(job_name):
    current_jobs_path = get_file_path("current_jobs.json")
    with open(current_jobs_path) as json_file:
        data = load(json_file)
        for job, job_id in data.items():
            if job == job_name:
                return job_id