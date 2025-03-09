import os
import json
import logging
import pymongo
import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import upper, udf, col, regexp_extract, input_file_name, lit
from pyspark.sql.types import StringType, StructType, StructField, IntegerType
from pyspark.sql.utils import AnalysisException
from datetime import datetime
from pathlib import Path
from urllib.parse import quote_plus
from unidecode import unidecode
from elasticsearch import Elasticsearch
try:
    from schema_mongodb import mongodb_schema_silver
except ModuleNotFoundError:
    from src.schemas.schema_mongodb import mongodb_schema_silver


# Função para remover acentos
def remove_accents(s):
    return unidecode(s)

remove_accents_udf = F.udf(remove_accents, StringType())

def log_error(e, df):
    """Gera e salva métricas de erro no Elastic."""

    # Convertendo "segmento" para uma lista de strings
    segmentos_unicos = [row["segmento"] for row in df.select("segmento").distinct().collect()]

    error_metrics = {
        "timestamp": datetime.now().isoformat(),
        "layer": "silver",
        "project": "compass",
        "job": "mongodb_reviews",
        "priority": "0",
        "tower": "SBBR_COMPASS",
        "client": segmentos_unicos,
        "error": str(e)
    }

    # Serializa para JSON e salva no MongoDB
    save_metrics_job_fail(json.dumps(error_metrics))

def read_source_parquet(spark, path):
    """Tenta ler um Parquet e retorna None se não houver dados"""
    try:
        df = spark.read.parquet(path)
        if df.isEmpty():
            print(f"[*] Nenhum dado encontrado em: {path}")
            return None
        return df.withColumn("app", regexp_extract(input_file_name(), "/mongodb/(.*?)/odate=", 1)) \
            .withColumn("segmento", regexp_extract(input_file_name(), r"/mongodb/[^/_]+_([pfj]+)/odate=", 1))
    except AnalysisException:
        print(f"[*] Falha ao ler: {path}. O arquivo pode não existir.")
        return None

def processing_reviews(df: DataFrame):

    logging.info(f"{datetime.now().strftime('%Y%m%d %H:%M:%S.%f')} [*] Processando o tratamento da camada historica")

    # Aplicando as transformações no DataFrame
    df_select = df.select(
        "id",
        "cpf",
        "customer_id",
        "age",
        "segmento",
        "app",
        "os",
        "os_version",
        "country",
        "app_version",
        "rating",
        "votes_count",
        "timestamp",
        F.upper(remove_accents_udf(F.col("comment"))).alias("comment")
    )

    return df_select

def save_dataframe(df, path, label):
    """
    Salva o DataFrame em formato parquet e loga a operação.
    """
    try:
        schema = mongodb_schema_silver()
        # Alinhar o DataFrame ao schema definido
        df = get_schema(df, schema)

        if df.limit(1).count() > 0:  # Verificar existência de dados
            logging.info(f"[*] Salvando dados {label} para: {path}")
            # Verifica se o diretório existe e cria-o se não existir
            Path(path).mkdir(parents=True, exist_ok=True)

            df.write.option("compression", "snappy").mode("overwrite").parquet(path)
            logging.info(f"[*] Dados salvos em {path} no formato Parquet")
        else:
            logging.warning(f"[*] Nenhum dado {label} foi encontrado!")
    except Exception as e:
        logging.error(f"[*] Erro ao salvar dados {label}: {e}", exc_info=True)
        log_error(e, df)
        

def get_schema(df, schema):
    """
    Obtém o DataFrame a seguir o schema especificado.
    """
    for field in schema.fields:
        if field.dataType == IntegerType():
            df = df.withColumn(field.name, df[field.name].cast(IntegerType()))
        elif field.dataType == StringType():
            df = df.withColumn(field.name, df[field.name].cast(StringType()))
    return df.select([field.name for field in schema.fields])


def processing_old_new(spark: SparkSession, df: DataFrame):
    """
    Compara os dados novos de avaliações de um aplicativo com os dados antigos já armazenados.
    Quando há diferenças entre os novos e os antigos, essas mudanças são salvas em uma nova coluna chamada 'historical_data'.
    O objetivo é manter um registro das alterações nas avaliações ao longo do tempo, permitindo ver como os dados evoluíram.


    Args:
        spark (SparkSession): A sessão ativa do Spark para ler e processar os dados.
        df (DataFrame): DataFrame contendo os novos dados de reviews.

    Returns:
        DataFrame: DataFrame resultante contendo os dados combinados e o histórico de mudanças.
    """

    # Obtenha a data atual
    current_date = datetime.now().strftime("%Y-%m-%d")

    # Caminho para os dados históricos
    historical_data_path = f"/santander/silver/compass/reviews/mongodb/"
   
    # Verificando se o caminho existe no HDFS
    path_exists = os.system(f"hadoop fs -test -e {historical_data_path} ") == 0

    if path_exists:
        # Se o caminho existir, lê os dados do Parquet e ajusta os tipos
        df_historical = (spark.read.option("basePath", historical_data_path)
                           .parquet(f"{historical_data_path}odate=*")
                           .withColumn("odate", F.to_date(F.col("odate"), "yyyyMMdd"))
                           .filter(f"odate < '{current_date}'")  # Não considerar o odate atual em caso de reprocessamento
                           .drop("odate"))
        
        # Adiciona a coluna 'response' se não existir no DataFrame
        if 'response' not in df.columns:
            df_historical = df_historical.withColumn("response", F.lit(None))

        if 'segmento' not in df.columns:
            df_historical = df_historical.withColumn("segmento", F.lit("NA"))

    else:
        # Se o caminho não existir, cria um DataFrame vazio com o mesmo esquema esperado
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("age", StringType(), True),
            StructField("app", StringType(), True),
            StructField("app_version", StringType(), True),
            StructField("comment", StringType(), True),
            StructField("country", StringType(), True),   
            StructField("cpf", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("ip_address", StringType(), True),
            StructField("os", StringType(), True),
            StructField("segmento", StringType(), True),
            StructField("os_version", StringType(), True),
            StructField("rating", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("votes_count", StringType(), True),
            StructField("odate", StringType(), True)
        ])

        df_historical = spark.createDataFrame([], schema)

    df_historical.printSchema()

    # Definindo aliases para os DataFrames
    new_reviews_df_alias = df.withColumn("cpf_n", F.col("cpf")).alias("new")  # DataFrame de novos reviews
    historical_reviews_df_alias = df_historical.alias("old")  # DataFrame de reviews históricos

    # Junção dos DataFrames
    joined_reviews_df = new_reviews_df_alias.join(historical_reviews_df_alias, new_reviews_df_alias.cpf_n == historical_reviews_df_alias.cpf, "outer")


    # Criação da coluna historical_data
    result_df_historical = joined_reviews_df.withColumn(
        "historical_data_temp",
        F.when(
            (F.col("new.customer_id").isNotNull()) & (F.col("old.customer_id").isNotNull()) & (F.col("new.customer_id") != F.col("old.customer_id")),
            F.array(F.struct(
                F.col("old.customer_id").alias("customer_id"),
                F.col("old.cpf").alias("cpf"),
                F.col("old.app").alias("app"),
                F.col("old.comment").alias("comment"),
                F.col("new.segmento").alias("segmento"),
                F.col("old.rating").cast("string").alias("rating"),
                F.col("old.timestamp").alias("timestamp"),
                F.col("old.app_version").alias("app_version"),
                F.col("old.os_version").alias("os_version"),
                F.col("old.os").alias("os")
            ))
        ).when(
            (F.col("new.cpf_n").isNotNull()) & (F.col("old.cpf").isNotNull()) & (F.col("new.cpf_n") != F.col("old.cpf")),
            F.array(F.struct(
                F.col("old.customer_id").alias("customer_id"),
                F.col("old.cpf").alias("cpf"),
                F.col("old.app").alias("app"),
                F.col("old.comment").alias("comment"),
                F.col("new.segmento").alias("segmento"),
                F.col("old.rating").cast("string").alias("rating"),
                F.col("old.timestamp").alias("timestamp"),
                F.col("old.app_version").alias("app_version"),
                F.col("old.os_version").alias("os_version"),
                F.col("old.os").alias("os")
            ))
        ).when(
            (F.col("new.app").isNotNull()) & (F.col("old.app").isNotNull()) & (F.col("new.app") != F.col("old.app")),
            F.array(F.struct(
                F.col("old.customer_id").alias("customer_id"),
                F.col("old.cpf").alias("cpf"),
                F.col("old.app").alias("app"),
                F.col("old.comment").alias("comment"),
                F.col("new.segmento").alias("segmento"),
                F.col("old.rating").cast("string").alias("rating"),
                F.col("old.timestamp").alias("timestamp"),
                F.col("old.app_version").alias("app_version"),
                F.col("old.os_version").alias("os_version"),
                F.col("old.os").alias("os")
            ))
        ).when(
            (F.col("new.comment").isNotNull()) & (F.col("old.comment").isNotNull()) & (F.col("new.comment") != F.col("old.comment")),
            F.array(F.struct(
                F.col("old.customer_id").alias("customer_id"),
                F.col("old.cpf").alias("cpf"),
                F.col("old.app").alias("app"),
                F.col("old.comment").alias("comment"),
                F.col("new.segmento").alias("segmento"),
                F.col("old.rating").cast("string").alias("rating"),
                F.col("old.timestamp").alias("timestamp"),
                F.col("old.app_version").alias("app_version"),
                F.col("old.os_version").alias("os_version"),
                F.col("old.os").alias("os")
            ))
        ).when(
            (F.col("new.rating").isNotNull()) & (F.col("old.rating").isNotNull()) & (F.col("new.rating") != F.col("old.rating")),
            F.array(F.struct(
                F.col("old.customer_id").alias("customer_id"),
                F.col("old.cpf").alias("cpf"),
                F.col("old.app").alias("app"),
                F.col("old.comment").alias("comment"),
                F.col("new.segmento").alias("segmento"),
                F.col("old.rating").cast("string").alias("rating"),
                F.col("old.timestamp").alias("timestamp"),
                F.col("old.app_version").alias("app_version"),
                F.col("old.os_version").alias("os_version"),
                F.col("old.os").alias("os")
            ))
        ).when(
            (F.col("new.timestamp").isNotNull()) & (F.col("old.timestamp").isNotNull()) & (F.col("new.timestamp") != F.col("old.timestamp")),
            F.array(F.struct(
                F.col("old.customer_id").alias("customer_id"),
                F.col("old.cpf").alias("cpf"),
                F.col("old.app").alias("app"),
                F.col("old.comment").alias("comment"),
                F.col("new.segmento").alias("segmento"),
                F.col("old.rating").cast("string").alias("rating"),
                F.col("old.timestamp").alias("timestamp"),
                F.col("old.app_version").alias("app_version"),
                F.col("old.os_version").alias("os_version"),
                F.col("old.os").alias("os")
            ))
        ).when(
            (F.col("new.app_version").isNotNull()) & (F.col("old.app_version").isNotNull()) & (F.col("new.app_version") != F.col("old.app_version")),
            F.array(F.struct(
                F.col("old.customer_id").alias("customer_id"),
                F.col("old.cpf").alias("cpf"),
                F.col("old.app").alias("app"),
                F.col("old.comment").alias("comment"),
                F.col("new.segmento").alias("segmento"),
                F.col("old.rating").cast("string").alias("rating"),
                F.col("old.timestamp").alias("timestamp"),
                F.col("old.app_version").alias("app_version"),
                F.col("old.os_version").alias("os_version"),
                F.col("old.os").alias("os")
            ))
        ).when(
            (F.col("new.os_version").isNotNull()) & (F.col("old.os_version").isNotNull()) & (F.col("new.os_version") != F.col("old.os_version")),
            F.array(F.struct(
                F.col("old.customer_id").alias("customer_id"),
                F.col("old.cpf").alias("cpf"),
                F.col("old.app").alias("app"),
                F.col("old.comment").alias("comment"),
                F.col("new.segmento").alias("segmento"),
                F.col("old.rating").cast("string").alias("rating"),
                F.col("old.timestamp").alias("timestamp"),
                F.col("old.app_version").alias("app_version"),
                F.col("old.os_version").alias("os_version"),
                F.col("old.os").alias("os")
            ))
        ).when(
            (F.col("new.os").isNotNull()) & (F.col("old.os").isNotNull()) & (F.col("new.os") != F.col("old.os")),
            F.array(F.struct(
                F.col("old.customer_id").alias("customer_id"),
                F.col("old.cpf").alias("cpf"),
                F.col("old.app").alias("app"),
                F.col("old.comment").alias("comment"),
                F.col("new.segmento").alias("segmento"),
                F.col("old.rating").cast("string").alias("rating"),
                F.col("old.timestamp").alias("timestamp"),
                F.col("old.app_version").alias("app_version"),
                F.col("old.os_version").alias("os_version"),
                F.col("old.os").alias("os")
            ))
        )
    ).distinct()

    result_df_historical.printSchema()

    # Agrupando e coletando históricos
    df_final = result_df_historical.groupBy("new.id").agg(
        F.coalesce(F.first(F.col("new.customer_id")), F.first(F.col("old.customer_id"))).alias("customer_id"),
        F.coalesce(F.first(F.col("new.cpf_n")), F.first(F.col("old.cpf"))).alias("cpf"),
        F.coalesce(F.first(F.col("new.app")), F.first(F.col("old.app"))).alias("app"),
        F.coalesce(F.first(F.col("new.rating")), F.first(F.col("old.rating"))).alias("rating"),
        F.coalesce(F.first(F.col("new.timestamp")), F.first(F.col("old.timestamp"))).alias("timestamp"),
        F.coalesce(F.first(F.col("new.comment")), F.first(F.col("old.comment"))).alias("comment"),
        F.coalesce(F.first(F.col("new.app_version")), F.first(F.col("old.app_version"))).alias("app_version"),
        F.coalesce(F.first(F.col("new.os_version")), F.first(F.col("old.os_version"))).alias("os_version"),
        F.coalesce(F.first(F.col("new.os")), F.first(F.col("old.os"))).alias("os"),
        F.first("new.segmento").alias("segmento"),
        F.flatten(F.collect_list("historical_data_temp")).alias("historical_data")
    )


    logging.info(f"[*] Visao final do processing_reviews", exc_info=True)
    df_final.show()

    return df_final

def save_metrics_job_fail(metrics_json):
    """
    Salva as métricas de aplicações com falhas
    """

    ES_HOST = "http://elasticsearch:9200"
    ES_INDEX = "compass_dt_datametrics_fail"
    ES_USER = os.environ["ES_USER"]
    ES_PASS = os.environ["ES_PASS"]

    # Conectar ao Elasticsearch
    es = Elasticsearch(
        [ES_HOST],
        basic_auth=(ES_USER, ES_PASS)
    )

    try:
        # Converter JSON em dicionário
        metrics_data = json.loads(metrics_json)

        # Inserir no Elasticsearch
        response = es.index(index=ES_INDEX, document=metrics_data)

        logging.info(f"[*] Métricas da aplicação salvas no Elasticsearch: {response}")
    except json.JSONDecodeError as e:
        logging.error(f"[*] Erro ao processar métricas: {e}", exc_info=True)
    except Exception as e:
        logging.error(f"[*] Erro ao salvar métricas no Elasticsearch: {e}", exc_info=True)
