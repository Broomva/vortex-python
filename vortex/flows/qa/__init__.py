from abc import abstractmethod
from vortex.datamodels.pipeline.qa import QA
from vortex.utils import fill_qa_json_template, to_json_object
from pydeequ.verification import VerificationResult, VerificationSuite
from vortex.engine.delta import DeltaEngine
from pyspark.sql.functions import current_timestamp, lit
from pydeequ.checks import CheckLevel, Check, CheckStatus
import ast
import copy
from datetime import datetime


class BatchQA:
    def __init__(self, params) -> None:
        self._params = self._set_params(params)
        self._sink = self._set_sink()

    @abstractmethod
    def qa_logic(self, dataframe, spark=None):
        pass

    @property
    def qa_enable(self):
        return self._params.qa_enable

    def _set_params(self, params):
        params = to_json_object(params)

        if params:
            params = fill_qa_json_template(values=params)[1]

        return QA(**params)

    def _set_sink(self):
        engine_params = {
            "database": self._params.qa_db,
            "table": self._params.qa_table,
            "mode": "append",
        }

        return DeltaEngine(engine_params)

    def run(self, dataframe, spark):
        def evaluate_operator(x, op, threshold, c):
            SAFE_NODES = (
                ast.Expression,
                ast.Compare,
                ast.Name,
                ast.Num,
                ast.Str,
                ast.Load,
                ast.GtE,
                ast.LtE,
                ast.Eq,
                ast.NotEq,
                ast.Gt,
                ast.Lt,
            )

            print("%%%%%", c, op, x, threshold)
            node = ast.parse(f"x {op} threshold", mode="eval")

            unsafe_nodes = [
                type(n).__name__
                for n in ast.walk(node)
                if not isinstance(n, SAFE_NODES)
            ]

            if unsafe_nodes:
                raise ValueError(
                    f"Unsafe nodes detected for column '{column}': {', '.join(unsafe_nodes)}"
                )

            return eval(compile(node, "<ast>", "eval"))

        if len(self._params.checks) > 0:
            check = Check(spark, CheckLevel.Error, self._params.check_name)
            for check_name, values in self._params.checks.items():
                if not isinstance(values, int):
                    list_checks = ast.literal_eval(values)
                    for check_tupla in list_checks:
                        column, *rest = check_tupla
                        op, threshold = rest if len(rest) == 2 else (">=", rest[0])

                        method_to_call = getattr(check, check_name)
                        validation = lambda x, current_op=op, c=column, t=threshold: evaluate_operator(
                            x, current_op, t, c
                        )

                        check = method_to_call(column, validation)

                else:
                    temp = copy.deepcopy(values)

                    check = getattr(check, check_name)(lambda x: x >= temp)

            check_result = (
                VerificationSuite(spark).onData(dataframe).addCheck(check).run()
            )

        else:
            check_result = self.qa_logic(dataframe, spark)

        timestamp = current_timestamp()
        qa_dataframe_result = VerificationResult.checkResultsAsDataFrame(
            spark, check_result
        ).withColumn("timestamp", lit(timestamp))
        self._sink.write_batch(qa_dataframe_result, spark)

        if (
            check_result.status != CheckStatus.Success.value
            and self._params.raise_alert
        ):
            print(qa_dataframe_result.display())
            raise Exception(
                f"""
                Verification Failed, timestamp: {datetime.now()}
                Check name: {self._params.check_name}
                Table:
                {qa_dataframe_result.show()}
                            """
            )
