from abc import ABC, abstractmethod


def create_function_from_string(
        path: str = "",
        func_code_str: str = "def process(dataframe, spark, params):\n    return dataframe",
):
    g = {}
    l = {}
    try:
        if path:
            with open(path, "r") as f:
                func_code_str = f.read()
                exec(func_code_str, g, l)
        elif func_code_str:
            exec(func_code_str, g, l)
        if l:
            return list(l.values())[0]
    except Exception as e:
        print(e)


class Pipeline(ABC):

    def __init__(self):
        pass
