# Databricks notebook source
from pyspark.sql import SparkSession

from vortex.flows.batch.builder import BatchETLPipelineBuilder
from vortex.orchestrator.parser import VortexManifestParser
from vortex.flows.batch.director import BatchETLPipelineDirector
from vortex.flows.processing import ParamsBusinessLogicTransform
from vortex.utils.runtime import set_runtime_options
from vortex.utils.udfs import register_udfs

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()
set_runtime_options(spark)
register_udfs(spark)

# COMMAND ----------

params = VortexManifestParser(spark=spark).databricks_params()

# COMMAND ----------

# Director creation
pipeline = BatchETLPipelineDirector()

# Processing creation
processing = ParamsBusinessLogicTransform()

# Builder creation
builder = BatchETLPipelineBuilder(processing)

# COMMAND ----------

# Builder Init
# %% Builder Init
builder.init(params=params)
pipeline.builder = builder


# COMMAND ----------

# %% Pipeline Execution
pipeline.etl(spark)
