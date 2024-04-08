# import pytest
# from unittest.mock import MagicMock, patch
# from vortex.QAE.pipeline_data_quality import BatchQA
# import findspark
# from pyspark.sql import SparkSession
#
# findspark.init()
#
#
# @pytest.fixture(scope="module")
# def spark():
#     spark = (
#         SparkSession.builder.appName("Deequ Test")
#         .config("spark.jars.packages", "com.amazon.deequ:deequ:2.0.4-spark-3.3")
#         .getOrCreate()
#     )
#     return spark
#
#
# @pytest.fixture
# def dummy_dataframe(spark):
#     data = [
#         ("FDCSITE_NAME_1", "TANK_STATION_NAME_1", "FDCSITE_CODE_1", "TANK_NAME_1"),
#         ("FDCSITE_NAME_2", "TANK_STATION_NAME_2", "FDCSITE_CODE_2", "TANK_NAME_2"),
#     ]
#     columns = ["FDCSITE_NAME", "TANK_STATION_NAME", "FDCSITE_CODE", "TANK_NAME"]
#     return spark.createDataFrame(data, columns)
#
#
# @pytest.fixture
# def qa_params():
#     return {
#         "checks": {
#             "hasSize": 59649,
#             "hasCompleteness": "[ ('FDCSITE_NAME',0.799), ('TANK_STATION_NAME','>=',  0.9), ('FDCSITE_CODE', '>=', 0.799), ('TANK_NAME', 0.9),]",
#         },
#         "raise_alert": "True",
#         "qa_db": "default",
#         "qa_table": "test_qa",
#         "qa_enable": "True",
#         "check_name": "vw_tsdb_tank_master_qa_checks_159",
#     }
#
#
# class TestBatchQA:
#     @patch("vortex.QAE.pipeline_data_quality.DeltaEngine")
#     @patch("vortex.QAE.pipeline_data_quality.VerificationSuite")
#     @patch("vortex.QAE.pipeline_data_quality.VerificationResult")
#     def test_run_with_successful_check(
#         self,
#         MockVerificationResult,
#         MockVerificationSuite,
#         MockDeltaEngine,
#         spark,
#         dummy_dataframe,
#         qa_params,
#     ):
#         mock_check_result = MagicMock()
#         mock_check_result.status = "Success"
#         MockVerificationResult.checkResultsAsDataFrame.return_value = dummy_dataframe
#         MockVerificationSuite.return_value.run.return_value = mock_check_result
#
#         batch_qa = BatchQA(qa_params)
#
#         batch_qa.run(dummy_dataframe, spark)
#
#         MockVerificationSuite.assert_called_once_with(spark)
#         MockVerificationResult.checkResultsAsDataFrame.assert_called_once_with(
#             spark, mock_check_result
#         )
#         dummy_dataframe.withColumn.assert_called_once()
