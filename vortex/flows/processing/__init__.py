from abc import ABC, abstractmethod

class Processing(ABC):
    """
    The Builder interface specifies methods for creating the different parts of
    the Product objects.
    """

    @abstractmethod
    def format_data(self, dataframe, payload_schema):
        pass

    @abstractmethod
    def payload_schema(self):
        pass

    @abstractmethod
    def enrich_streaming(self, dataframe, spark, params=None):
        pass


class ParamsBusinessLogicTransform(Processing):
    def transformation(self, dataframe=None, spark=None, params: dict = None):
        def create_func_obj(func_code_str):
            g = {}
            l = {}
            exec(func_code_str, g, l)
            if l:
                return list(l.values())[0]

        print(f"Received params: {params}")
        if str(params["logic_type"]) == "sql":
            transform_logic = str(params["business_logic"])
            dataframe.createOrReplaceTempView("source")
            print(
                f"Executing SQL Statement from business logic param: {transform_logic}"
            )
            return spark.sql(transform_logic)
        elif str(params["logic_type"]) == "python":
            dataframe = dataframe
            f = create_func_obj(str(params["business_logic"]))
            print(
                f"Executing Python Statement from business logic param: {params['business_logic']}"
            )
            return f(dataframe)
        elif str(params["logic_type"]) == "sql_file":
            with open(str(params["business_logic"]), "r") as f:
                query = f.read()
            transform_logic = str(query)
            print(
                f"Executing SQL Statement from business logic param: {transform_logic}"
            )
            return spark.sql(transform_logic)


def transformation(dataframe=None, spark=None, params: dict = None):
    def create_func_obj(func_code_str):
        g = {}
        l = {}
        exec(func_code_str, g, l)
        if l:
            return list(l.values())[0]

    logic_type = str(params["logic_type"])
    business_logic = str(params["business_logic"])
    dataframe.createOrReplaceTempView("source")

    if logic_type in {"sql", "sql_file"}:
        if logic_type == "sql_file":
            with open(business_logic, "r") as f:
                transform_logic = f.read()
        else:
            transform_logic = business_logic
        print(f"Executing SQL Statement from business logic param: {transform_logic}")
        return spark.sql(transform_logic)

    elif logic_type in {"python", "python_file"}:
        if logic_type == "python_file":
            with open(business_logic, "r") as f:
                transform_logic = f.read()
        else:
            transform_logic = business_logic

        f = create_func_obj(transform_logic)
        print(
            f"Executing Python Statement from business logic param: {transform_logic}"
        )
        return f(**{"dataframe": dataframe, "spark": spark, "params": params})