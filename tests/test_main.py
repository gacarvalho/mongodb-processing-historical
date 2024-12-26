
import pytest
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from datetime import datetime
from unittest.mock import MagicMock, patch
from src.utils.tools import *
from src.metrics.metrics import validate_ingest

@pytest.fixture(scope="session")
def spark():
    """
    Fixture que inicializa o SparkSession para os testes.
    """
    spark = SparkSession.builder.master("local").appName("TestApp").getOrCreate()
    yield spark
    spark.stop()

# Função para retornar o schema MongoDB
def mongodb_schema_bronze():
    return StructType([
        StructField('id', StringType(), True),
        StructField('comment', StringType(), True),
        StructField('votes_count', IntegerType(), True),
        StructField('os', StringType(), True),
        StructField('os_version', StringType(), True),
        StructField('country', StringType(), True),
        StructField('age', IntegerType(), True),
        StructField('customer_id', StringType(), True),
        StructField('cpf', StringType(), True),
        StructField('app_version', StringType(), True),
        StructField('rating', IntegerType(), True),
        StructField('timestamp', StringType(), True),
        StructField('app', StringType(), True)
    ])



# Configuração do pytest para o Spark
@pytest.fixture(scope="session")
def spark():
    # Inicializar uma SparkSession para os testes
    return SparkSession.builder.master("local[1]").appName("TestReadData").getOrCreate()

def data_mongodb():
    return [
        {"id": "{674b9aa4529a065f5b78ad76}", "comment": "Excelente, super recomendo!", "votes_count": 2, "os": "Plataforma", "os_version": "10.0", "country": "BR", "age": 48, "customer_id": "5763", "cpf": "582.403.619-51", "app_version": "1.0.0", "rating": 2, "timestamp": "2024-11-30T23:07:02.251424", "app": "App1"},
        {"id": "{674b9aa4529a065f5b78ad77}", "comment": "Adorei a nova atualização!", "votes_count": 7, "os": "Android", "os_version": "10.0", "country": "BR", "age": 49, "customer_id": "4811", "cpf": "964.253.710-99", "app_version": "1.0.0", "rating": 4, "timestamp": "2024-11-30T23:07:02.251424", "app": "App1"},
        {"id": "{674b9aa4529a065f5b78ad78}", "comment": "O aplicativo é bom, mas pode melhorar.", "votes_count": 2, "os": "IOS", "os_version": "18.04", "country": "BR", "age": 19, "customer_id": "9022", "cpf": "186.320.579-95", "app_version": "1.0.0", "rating": 4, "timestamp": "2024-11-30T23:07:02.251424", "app": "App1"},
        {"id": "{674b9aa4529a065f5b78ad79}", "comment": "O app é bom, mas poderia ter mais opções.", "votes_count": 3, "os": "Android", "os_version": "11.0", "country": "BR", "age": 24, "customer_id": "6745", "cpf": "175.537.217-05", "app_version": "1.0.0", "rating": 3, "timestamp": "2024-11-30T23:07:02.251424", "app": "App1"},
        {"id": "{674b9aa4529a065f5b78ad80}", "comment": "Excelente! Recomendo a todos.", "votes_count": 8, "os": "IOS", "os_version": "18.04", "country": "BR", "age": 29, "customer_id": "4427", "cpf": "345.271.912-35", "app_version": "1.0.0", "rating": 5, "timestamp": "2024-11-30T23:07:02.251424", "app": "App1"},
        {"id": "{674b9aa4529a065f5b78ad81}", "comment": "Muito bom, mas pode melhorar a estabilidade.", "votes_count": 3, "os": "Plataforma", "os_version": "10.0", "country": "BR", "age": 45, "customer_id": "3045", "cpf": "147.934.302-14", "app_version": "1.0.0", "rating": 3, "timestamp": "2024-11-30T23:07:02.251424", "app": "App1"},
        {"id": "{674b9aa4529a065f5b78ad82}", "comment": "App excelente, funciona muito bem.", "votes_count": 1, "os": "Android", "os_version": "10.0", "country": "BR", "age": 32, "customer_id": "9094", "cpf": "815.872.003-98", "app_version": "1.0.0", "rating": 5, "timestamp": "2024-11-30T23:07:02.251424", "app": "App1"},
        {"id": "{674b9aa4529a065f5b78ad83}", "comment": "Faltam algumas funcionalidades importantes.", "votes_count": 0, "os": "IOS", "os_version": "18.04", "country": "BR", "age": 21, "customer_id": "4572", "cpf": "935.738.478-85", "app_version": "1.0.0", "rating": 2, "timestamp": "2024-11-30T23:07:02.251424", "app": "App1"},
        {"id": "{674b9aa4529a065f5b78ad84}", "comment": "Recomendo, mas precisa de melhorias.", "votes_count": 9, "os": "Plataforma", "os_version": "10.0", "country": "BR", "age": 36, "customer_id": None, "cpf": "374.892.590-99", "app_version": "1.0.0", "rating": 4, "timestamp": "2024-11-30T23:07:02.251424", "app": "App1"},
        {"id": "{674b9aa4529a065f5b78ad84}", "comment": "Recomendo, mas precisa de melhorias.", "votes_count": 9, "os": "Plataforma", "os_version": "10.0", "country": "BR", "age": 36, "customer_id": "3109", "cpf": "374.892.590-99", "app_version": "1.0.0", "rating": 4, "timestamp": "2024-11-30T23:07:02.251424", "app": "App1"},
        {"id": "{674b9aa4529a065f5b78ad85}", "comment": "Aplicativo bom, mas com alguns bugs.", "votes_count": 5, "os": "Android", "os_version": "9.0", "country": "BR", "age": 26, "customer_id": "7091", "cpf": "539.861.092-24", "app_version": "1.0.0", "rating": 4, "timestamp": "2024-11-30T23:07:02.251424", "app": "App1"},
        {"id": "{674b9aa4529a065f5b78ad86}", "comment": "Adorei o novo design do aplicativo.", "votes_count": 1, "os": "IOS", "os_version": "18.04", "country": "BR", "age": 31, "customer_id": "1620", "cpf": "273.794.915-89", "app_version": "1.0.0", "rating": 5, "timestamp": "2024-11-30T23:07:02.251424", "app": "App1"},
        {"id": "{674b9aa4529a065f5b78ad87}", "comment": "Faltam mais funcionalidades, mas funciona.", "votes_count": 3, "os": "Android", "os_version": "11.0", "country": "BR", "age": 33, "customer_id": "8839", "cpf": "883.019.116-99", "app_version": "1.0.0", "rating": 3, "timestamp": "2024-11-30T23:07:02.251424", "app": "App1"},
        {"id": "{674b9aa4529a065f5b78ad88}", "comment": "Bom app, mas a navegação poderia ser melhor.", "votes_count": 1, "os": "IOS", "os_version": "18.04", "country": "BR", "age": 20, "customer_id": "5555", "cpf": "997.681.003-09", "app_version": "1.0.0", "rating": 3, "timestamp": "2024-11-30T23:07:02.251424", "app": "App1"},
        {"id": "{674b9aa4529a065f5b78ad89}", "comment": "A interface poderia ser mais amigável.", "votes_count": 0, "os": "Android", "os_version": "10.0", "country": "BR", "age": 22, "customer_id": "3564", "cpf": "219.369.480-11", "app_version": "1.0.0", "rating": 2, "timestamp": "2024-11-30T23:07:02.251424", "app": "App1"},
        {"id": "{674b9aa4529a065f5b78ad90}", "comment": "Funciona bem, mas precisa de mais opções.", "votes_count": 4, "os": "Plataforma", "os_version": "10.0", "country": "BR", "age": 27, "customer_id": "8003", "cpf": "195.672.803-13", "app_version": "1.0.0", "rating": 3, "timestamp": "2024-11-30T23:07:02.251424", "app": "App1"}
    ]


# Teste unitário para a função read_data
def test_read_data(spark):
    # Criando um DataFrame de teste com dados fictícios
    test_data = data_mongodb()

    schema = mongodb_schema_bronze()
    # Criar o DataFrame com os dados de teste
    df_test = spark.createDataFrame(test_data, schema)

    # Salvar o DataFrame como um arquivo Parquet temporário
    datePath = datetime.now().strftime("%Y%m%d")
    test_parquet_path = f"/tmp/test_mongodb_data/odate={datePath}/"
    df_test.write.mode("overwrite").parquet(test_parquet_path)

    df_processado = processing_reviews(spark, test_parquet_path)

    # Verifique se o número de registros no DataFrame é o esperado
    assert df_processado.count() == 16, f"Esperado 16 registros, mas encontrou {df_processado.count()}."


def test_processamento_reviews(spark):
    # Criando um DataFrame de teste com dados fictícios
    test_data = data_mongodb()

    schema = mongodb_schema_bronze()
    # Criar o DataFrame com os dados de teste
    df_test = spark.createDataFrame(test_data, schema)

    # Salvar o DataFrame como um arquivo Parquet temporário
    datePath = datetime.now().strftime("%Y%m%d")
    test_parquet_path = f"/tmp/test_mongodb_data/odate={datePath}/"
    df_test.write.mode("overwrite").parquet(test_parquet_path)

    df_processado = processing_reviews(spark, test_parquet_path)

    # Verifica se a volumetria é maior que 0 e o df está preenchido
    assert df_processado.count() > 0, "[*] Dataframe vazio"


def test_validate_ingest(spark):
    """
    Testa a função de validação de ingestão para garantir que os DataFrames têm dados e que a validação gera resultados.
    """
    # Criando um DataFrame de teste com dados fictícios
    test_data = data_mongodb()

    schema = mongodb_schema_bronze()
    # Criar o DataFrame com os dados de teste
    df_test = spark.createDataFrame(test_data, schema)

    # Salvar o DataFrame como um arquivo Parquet temporário
    datePath = datetime.now().strftime("%Y%m%d")
    test_parquet_path = f"/tmp/test_mongodb_data/odate={datePath}/"
    df_test.write.mode("overwrite").parquet(test_parquet_path)

    df_processado = processing_reviews(spark, test_parquet_path)


    # Valida o DataFrame e coleta resultados
    valid_df, invalid_df, validation_results = validate_ingest(spark, df_processado)

    assert valid_df.count() > 0, "[*] O DataFrame válido está vazio!"
    assert invalid_df.count() > 0, "[*] O DataFrame inválido está vazio!"
    assert len(validation_results) > 0, "[*] Não foram encontrados resultados de validação!"

    # Exibir resultados para depuração
    print("Testes realizados com sucesso!")
    print(f"Total de registros válidos: {valid_df.count()}")
    print(f"Total de registros inválidos: {invalid_df.count()}")
    print(f"Resultados da validação: {validation_results}")

def test_save_data(spark):
    # Criando um DataFrame de teste com dados fictícios
    test_data = data_mongodb()

    schema = mongodb_schema_bronze()
    # Criar o DataFrame com os dados de teste
    df_test = spark.createDataFrame(test_data, schema)

    # Salvar o DataFrame como um arquivo Parquet temporário
    datePath = datetime.now().strftime("%Y%m%d")
    test_parquet_path = f"/tmp/test_mongodb_data/odate={datePath}/"
    df_test.write.mode("overwrite").parquet(test_parquet_path)

    df_processado = processing_reviews(spark, test_parquet_path)


    # Valida o DataFrame e coleta resultados
    valid_df, invalid_df, validation_results = validate_ingest(spark, df_processado)

    # Definindo caminhos
    datePath = datetime.now().strftime("%Y%m%d")
    path_target = f"/tmp/fake/path/valid/odate={datePath}/"

    # Mockando o método parquet
    with patch("pyspark.sql.DataFrameWriter.parquet", MagicMock()) as mock_parquet:
        # Chamando a função a ser testada
        save_dataframe(valid_df, path_target, "bronze")

        # Verificando se o método parquet foi chamado com os caminhos corretos
        mock_parquet.assert_any_call(path_target)

    print("[*] Teste de salvar dados concluído com sucesso!")