def get_dbutils(spark):
    """
    If you're running in a Databricks notebook, use the Databricks dbutils module, otherwise use the
    IPython dbutils module

    :param spark: The SparkSession object
    :return: The dbutils object
    """
    try:
        from pyspark.dbutils import DBUtils

        dbutils = DBUtils(spark)
    except ImportError:
        import IPython

        dbutils = IPython.get_ipython().user_ns["dbutils"]
    return dbutils


def set_ipy_env():
    """
    It sets the environment variables for the current Jupyter kernel and runs some bash commands
    """
    from IPython import get_ipython
    import sys, os

    if "ipykernel" in sys.modules:
        ipy = get_ipython()
        ipy.run_line_magic("sx", "source ./env_vars.env")
        ARTIFACT_PATH = os.getenv("ARTIFACT_PATH")
        ipy.run_line_magic("sx", f"pip install {ARTIFACT_PATH}")
        ipy.run_line_magic("sx", "pip install pydantic")
        ipy.run_line_magic("sx", "pip install toml")


def get_widget_parameters(spark):
    dbutils = get_dbutils(spark)
    my_widgets = dbutils.notebook.entry_point.getCurrentBindings()
    return {key: my_widgets[key] for key in my_widgets}
