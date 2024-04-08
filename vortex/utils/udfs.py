import hashlib
import uuid
from json import dumps, loads

from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType, IntegerType, StringType


def register_udfs(spark):
    """
    Register the functions that will be used in the spark sql session

    :param spark: The SparkSession object
    """

    def char_find(s):
        """
        Finds the first occurrence of a character in a string

        :param s: The string to search
        :return: The index of the first occurrence of the character '_' in the string.
        """
        return s.rfind("_")

    # Register UDF
    spark.udf.register("char_pos", char_find, IntegerType())

    def is_numeric_type(s):
        """
        Checks if a string is numeric

        :param s: The string to check
        :return: The function is_numeric_type() is returning a boolean value.
        """
        try:
            float(s)
            return True
        except ValueError:
            return False

    # Register UDF
    spark.udf.register("is_numeric", is_numeric_type, BooleanType())

    def _json_parse(x):
        payload = loads(dumps(str(x))).replace("'", "")
        payload = payload.replace(r"\"", '"')
        payload = payload.replace(r"\\", "")
        payload = payload.replace(r'"[', "[")
        payload = payload.replace(r']"', "]")
        payload = payload.replace(r'"{', "{")
        payload = payload.replace(r'}"', "}")

        payload = payload.replace(r'":{"', r'":[{"')
        payload = payload.replace(r'"}},"', r'"}]}],"')

        payload = payload.replace(r'"},"', r'"}],"')

        return payload

    spark.udf.register("json_parse", _json_parse, StringType())

    def _get_uuid(x):
        m = hashlib.md5()
        m.update(x.encode("utf-8"))
        return str(uuid.UUID(m.hexdigest(), version=4)).upper()

    spark.udf.register("get_uuid", _get_uuid, StringType())


def get_udfs():
    def _get_uuid(x):
        m = hashlib.md5()
        m.update(x.encode("utf-8"))
        return str(uuid.UUID(m.hexdigest(), version=4)).upper()

    def _json_parse(x):
        payload = loads(dumps(str(x))).replace("'", "")
        payload = payload.replace(r"\"", '"')
        payload = payload.replace(r"\\", "")
        payload = payload.replace(r'"[', "[")
        payload = payload.replace(r']"', "]")
        payload = payload.replace(r'"{', "{")
        payload = payload.replace(r'}"', "}")

        payload = payload.replace(r'":{"', r'":[{"')
        payload = payload.replace(r'"}},"', r'"}]}],"')

        payload = payload.replace(r'"},"', r'"}],"')

        return payload

    get_uuid = udf(lambda x: _get_uuid(x))
    json_parse = udf(lambda x: _json_parse(x))

    return get_uuid, json_parse
