import pytest
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType
from datetime import datetime
from unittest.mock import MagicMock, patch
from pyspark.sql.functions import lit, col

# Importações ajustadas para sua estrutura de projeto
from src.utils.tools import processing_reviews, read_source_parquet, save_dataframe
from src.metrics.metrics import validate_ingest
from src.repo_trfmation_mongodb import main as mongodb_main

# Fixture do Spark para os testes
@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[1]") \
        .appName("MongoDBTests") \
        .config("spark.sql.shuffle.partitions", "1") \
        .getOrCreate()

# Schema para testes
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

# Fixture com dados reais do MongoDB
@pytest.fixture
def test_data(spark):
    data = [
        {"id": "{674b9aa4529a065f5b78ad76}", "comment": "Excelente, super recomendo!", "votes_count": 2, "os": "Plataforma", "os_version": "10.0", "country": "BR", "age": "48", "customer_id": "5763", "cpf": "582.403.619-51", "app_version": "1.0.0", "rating": 2.0, "timestamp": "2024-11-30T23:07:02.251424"},
        {"id": "{674b9aa4529a065f5b78ad77}", "comment": "Adorei a nova atualização!", "votes_count": 7, "os": "Android", "os_version": "10.0", "country": "BR", "age": "49", "customer_id": "4811", "cpf": "964.253.710-99", "app_version": "1.0.0", "rating": 4.0, "timestamp": "2024-11-30T23:07:02.251424"},
        {"id": "{674b9aa4529a065f5b78ad84}", "comment": "Recomendo, mas precisa de melhorias.", "votes_count": 9, "os": "Plataforma", "os_version": "10.0", "country": "BR", "age": "36", "customer_id": None, "cpf": "374.892.590-99", "app_version": "1.0.0", "rating": 4.0, "timestamp": "2024-11-30T23:07:02.251424"}
    ]
    return spark.createDataFrame(data, mongodb_schema_bronze())

def test_read_source_parquet(spark, test_data):
    """Testa a função de leitura de dados"""
    test_path = "/tmp/test_mongodb_data"
    test_data.write.mode("overwrite").parquet(test_path)

    result_df = read_source_parquet(spark, test_path)

    assert result_df is not None
    assert result_df.count() == 3
    assert "app" in result_df.columns
    assert "segmento" in result_df.columns

    shutil.rmtree(test_path)

def test_processing_reviews(spark, test_data):
    """Testa o processamento das reviews"""
    df_with_segment = test_data.withColumn("app", lit("test_app")) \
        .withColumn("segmento", lit("pf"))

    processed_df = processing_reviews(df_with_segment)

    assert processed_df.count() == df_with_segment.count()
    assert "comment" in processed_df.columns
    assert processed_df.first()["comment"] == "EXCELENTE, SUPER RECOMENDO!"
    assert "segmento" in processed_df.columns

def test_validate_ingest(spark, test_data):
    """Testa a validação dos dados"""
    df_with_all = test_data.withColumn("app", lit("test_app")) \
        .withColumn("segmento", lit("pf")) \
        .withColumn("historical_data", lit(None).cast("array<struct<customer_id:string,cpf:string,app:string,comment:string,rating:string,timestamp:string>>"))

    valid_df, invalid_df, results = validate_ingest(spark, df_with_all)

    assert valid_df.count() == 2
    assert invalid_df.count() == 1
    assert isinstance(results, dict)

def test_save_dataframe(spark, test_data):
    """Testa a função de salvamento diretamente"""
    df_with_all = test_data.withColumn("app", lit("test_app")) \
        .withColumn("segmento", lit("pf")) \
        .withColumn("historical_data", lit(None).cast("array<struct<customer_id:string,cpf:string,app:string,comment:string,rating:string,timestamp:string>>"))

    with patch("pyspark.sql.DataFrameWriter.parquet") as mock_parquet:
        save_dataframe(df_with_all, "/tmp/test_output", "test")
        mock_parquet.assert_called_once()

def test_main_function(spark, test_data, monkeypatch):
    """Testa a função principal com mocks"""
    # Mock para os argumentos de linha de comando
    monkeypatch.setattr('sys.argv', ['test_script.py', 'test'])

    # Mock para variáveis de ambiente
    monkeypatch.setenv('ES_USER', 'test_user')
    monkeypatch.setenv('ES_PASSWORD', 'test_password')

    # Mock para as funções internas
    with patch('src.repo_trfmation_mongodb.read_source_parquet') as mock_read, \
            patch('src.repo_trfmation_mongodb.processing_reviews') as mock_process, \
            patch('src.repo_trfmation_mongodb.validate_ingest') as mock_validate, \
            patch('src.repo_trfmation_mongodb.save_dataframe') as mock_save, \
            patch('src.repo_trfmation_mongodb.MetricsCollector') as mock_metrics, \
            patch('src.repo_trfmation_mongodb.log_error') as mock_log_error, \
            patch('src.repo_trfmation_mongodb.save_metrics') as mock_save_metrics:

        # Configurar os mocks
        mock_read.return_value = test_data
        mock_process.return_value = test_data
        mock_validate.return_value = (test_data, test_data.limit(0), {})

        # Mock para o MetricsCollector
        mock_metrics_instance = MagicMock()
        mock_metrics.return_value = mock_metrics_instance

        # Mock para save_metrics
        mock_save_metrics.return_value = None

        # Executar a função main
        mongodb_main()

        # Verificar se as funções foram chamadas
        mock_read.assert_called()
        mock_process.assert_called()
        mock_validate.assert_called()
        mock_save.assert_called()
        mock_metrics_instance.start_collection.assert_called()
        mock_metrics_instance.end_collection.assert_called()
        mock_save_metrics.assert_called()
        mock_log_error.assert_not_called()