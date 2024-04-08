# Databricks notebook source
from pyspark.sql import SparkSession

from vortex.orchestrator.parser import VortexManifestParser
from vortex.flows.streaming.selector import selector
from vortex.utils.runtime import set_runtime_options
from vortex.utils.udfs import register_udfs

# COMMAND ----------

# %%
spark = SparkSession.builder.getOrCreate()
set_runtime_options(spark)
register_udfs(spark)

# COMMAND ----------

# %%
job_params = VortexManifestParser(spark=spark).databricks_params()

# COMMAND ----------

# %% Pipele creation
pipeline = selector(job_params, spark)
