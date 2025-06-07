# tests/test_main.py
import pytest
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType, ArrayType
from datetime import datetime
from pyspark.sql.functions import lit, col
from unittest.mock import MagicMock, patch, PropertyMock
from src.utils.tools import processing_reviews, save_dataframe, processing_old_new, read_source_parquet
from src.metrics.metrics import validate_ingest, MetricsCollector

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[1]").appName("TestMongoDB").getOrCreate()

def mongodb_schema_bronze():
    return StructType([
        StructField("id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("cpf", StringType(), True),
        StructField("age", StringType(), True),
        StructField("os", StringType(), True),
        StructField("os_version", StringType(), True),
        StructField("country", StringType(), True),
        StructField("app_version", StringType(), True),
        StructField("rating", DoubleType(), True),
        StructField("votes_count", LongType(), True),
        StructField("timestamp", StringType(), True),
        StructField("comment", StringType(), True)
    ])

def mongodb_schema_silver():
    return StructType([
        StructField("id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("cpf", StringType(), True),
        StructField("app", StringType(), True),
        StructField("segmento", StringType(), True),
        StructField("rating", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("comment", StringType(), True),
        StructField("app_version", StringType(), True),
        StructField("os_version", StringType(), True),
        StructField("os", StringType(), True),
        StructField("historical_data", ArrayType(ArrayType(StructType([
            StructField("customer_id", StringType(), True),
            StructField("cpf", StringType(), True),
            StructField("app", StringType(), True),
            StructField("segmento", StringType(), True),
            StructField("comment", StringType(), True),
            StructField("rating", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("app_version", StringType(), True),
            StructField("os_version", StringType(), True),
            StructField("os", StringType(), True)
        ]), True), True), True)
    ])

def test_data_bronze():
    return [
        {
            "id": "674b9aa4529a065f5b78ad76",
            "customer_id": "5763",
            "cpf": "58240361951",
            "age": "48",
            "os": "Android",
            "os_version": "10.0",
            "country": "BR",
            "app_version": "1.0.0",
            "rating": 5.0,
            "votes_count": 2,
            "timestamp": "2024-11-30T23:07:02.251424",
            "comment": "Excelente, super recomendo!"
        },
        {
            "id": "674b9aa4529a065f5b78ad77",
            "customer_id": "4811",
            "cpf": "96425371099",
            "age": "49",
            "os": "iOS",
            "os_version": "15.0",
            "country": "BR",
            "app_version": "1.0.0",
            "rating": 4.0,
            "votes_count": 7,
            "timestamp": "2024-11-30T23:07:02.251424",
            "comment": "Adorei a nova atualização!"
        }
    ]

def test_data_silver():
    return [
        {
            "id": "674b9aa4529a065f5b78ad76",
            "customer_id": "5763",
            "cpf": "58240361951",
            "app": "test_app",
            "segmento": "pf",
            "rating": "5",
            "timestamp": "2024-11-30T23:07:02.251424",
            "comment": "EXCELENTE, SUPER RECOMENDO!",
            "app_version": "1.0.0",
            "os_version": "10.0",
            "os": "Android",
            "historical_data": None
        }
    ]

def test_read_data(spark):
    df_test = spark.createDataFrame(test_data_bronze(), mongodb_schema_bronze())
    date_path = datetime.now().strftime("%Y%m%d")
    test_parquet_path = f"/tmp/test_mongodb_data/mongodb/test_app_pf/odate={date_path}/"

    # Write test data
    (df_test
     .withColumn("app", lit("test_app"))
     .withColumn("segmento", lit("pf"))
     .write.mode("overwrite")
     .parquet(test_parquet_path))

    # Test reading
    df = read_source_parquet(spark, test_parquet_path)
    assert df.count() == 2
    assert "app" in df.columns
    assert "segmento" in df.columns

def test_processing_reviews(spark):
    df_test = spark.createDataFrame(test_data_bronze(), mongodb_schema_bronze())
    df_with_segment = df_test.withColumn("app", lit("test_app")).withColumn("segmento", lit("pf"))
    df_processed = processing_reviews(df_with_segment)

    assert df_processed.count() == 2
    assert df_processed.first()["comment"] == "EXCELENTE, SUPER RECOMENDO!"
    assert "segmento" in df_processed.columns

def test_processing_old_new(spark):
    df_test = spark.createDataFrame(test_data_bronze(), mongodb_schema_bronze())
    df_with_segment = df_test.withColumn("app", lit("test_app")).withColumn("segmento", lit("pf"))

    # Test with no historical data
    result = processing_old_new(spark, df_with_segment)
    assert result.count() == 2

def test_validate_ingest(spark):
    df_test = spark.createDataFrame(test_data_silver(), mongodb_schema_silver())
    valid_df, invalid_df, validation_results = validate_ingest(spark, df_test)

    assert valid_df.count() == 1
    assert invalid_df.count() == 0
    assert "duplicate_check" in validation_results
    assert "null_check" in validation_results
    assert "type_consistency_check" in validation_results

def test_save_data(spark):
    from src.utils import tools  # importa dentro do teste para patch correto

    df = spark.createDataFrame(test_data_bronze(), mongodb_schema_bronze()).withColumn("app", lit("banco-santander-br")).withColumn("segmento", lit("pf"))
    df_processado = processing_reviews(df)
    valid_df, _, _ = validate_ingest(spark, df_processado)
    datePath = datetime.now().strftime("%Y%m%d")
    path_target = f"/tmp/fake/path/valid/odate={datePath}"

    mock_parquet = MagicMock()
    mock_partitionBy = MagicMock()
    mock_partitionBy.parquet = mock_parquet

    mock_mode = MagicMock()
    mock_mode.partitionBy.return_value = mock_partitionBy

    mock_option = MagicMock()
    mock_option.mode.return_value = mock_mode

    mock_write = MagicMock()
    mock_write.option.return_value = mock_option

    with patch.object(type(valid_df), "write", new_callable=PropertyMock) as mock_write_property:
        mock_write_property.return_value = mock_write

        tools.save_dataframe(
            df=valid_df,
            path=path_target,
            label="valido",
            partition_column="odate",
            compression="snappy"
        )

        mock_write.option.assert_called_once_with("compression", "snappy")
        mock_option.mode.assert_called_once_with("overwrite")
        mock_mode.partitionBy.assert_called_once_with("odate")
        mock_parquet.assert_called_once_with(path_target)