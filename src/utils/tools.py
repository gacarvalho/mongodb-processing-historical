import os
import json
import logging
import pymongo
import traceback
import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import upper, udf, col, regexp_extract, input_file_name, lit
from pyspark.sql.types import StringType, StructType, StructField, IntegerType
from pyspark.sql.utils import AnalysisException
from datetime import datetime
from pathlib import Path
from urllib.parse import quote_plus
from unidecode import unidecode
from typing import Optional, Union
from elasticsearch import Elasticsearch
try:
    from schema_mongodb import mongodb_schema_silver
except ModuleNotFoundError:
    from src.schemas.schema_mongodb import mongodb_schema_silver

# Logging estruturado
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(message)s"
)

logger = logging.getLogger(__name__)


# Função para remoção de acentos
def remove_accents(s):
    """Remove acentos e caracteres especiais de uma string"""
    return unidecode(s)

remove_accents_udf = F.udf(remove_accents, StringType())


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

def save_dataframe(
        df: DataFrame,
        path: str,
        label: str,
        schema: Optional[StructType] = None,
        partition_column: str = "odate",
        compression: str = "snappy"
) -> bool:
    """
    Salva um DataFrame Spark no formato Parquet de forma robusta.

    Args:
        df: DataFrame a ser salvo
        path: Caminho de destino
        label: Identificação para logs (ex: 'valido', 'invalido')
        schema: Schema opcional para validação
        partition_column: Coluna de partição
        compression: Tipo de compressão

    Returns:
        bool: True se salvou com sucesso, False caso contrário

    Raises:
        ValueError: Se os parâmetros forem inválidos
        IOError: Se houver problemas ao escrever no filesystem
    """
    if not isinstance(df, DataFrame):
        logger.error(f"[*] Objeto passado não é um DataFrame Spark: {type(df)}")
        return False

    if not path:
        logger.error("Caminho de destino não pode ser vazio")
        return False

    current_date = datetime.now().strftime('%Y%m%d')
    full_path = Path(path)

    try:
        if schema:
            logger.info(f"[*] Aplicando schema para dados {label}")
            df = get_schema(df, schema)

        df_partition = df.withColumn(partition_column, lit(current_date))

        if not df_partition.head(1):
            logger.warning(f"[*] Nenhum dado {label} encontrado para salvar")
            return False

        try:
            full_path.mkdir(parents=True, exist_ok=True)
            logger.debug(f"[*] Diretório {full_path} verificado/criado")
        except Exception as dir_error:
            logger.error(f"[*] Falha ao preparar diretório {full_path}: {dir_error}")
            raise IOError(f"[*] Erro de diretório: {dir_error}") from dir_error

        logger.info(f"[*] Salvando {df_partition.count()} registros ({label}) em {full_path}")

        (df_partition.write
         .option("compression", compression)
         .mode("overwrite")
         .partitionBy(partition_column)
         .parquet(str(full_path)))

        logger.info(f"[*] Dados {label} salvos com sucesso em {full_path}")
        return True

    except Exception as e:
        error_msg = f"[*] Falha ao salvar dados {label} em {full_path}"
        logger.error(error_msg, exc_info=True)
        logger.error(f"[*] Detalhes do erro: {str(e)}\n{traceback.format_exc()}")
        return False

def save_metrics(
        metrics_type: str,
        index: str,
        error: Optional[Exception] = None,
        df: Optional[DataFrame] = None,
        metrics_data: Optional[Union[dict, str]] = None
) -> None:
    """
    Salva métricas no Elasticsearch com estruturas específicas.

    Args:
        metrics_type: 'success' ou 'fail'
        index: Nome do índice no Elasticsearch
        error: Objeto de exceção (para tipo 'fail')
        df: DataFrame (para extrair segmentos)
        metrics_data: Dados das métricas (para tipo 'success')

    Raises:
        ValueError: Se os parâmetros forem inválidos
    """
    metrics_type = metrics_type.lower()

    if metrics_type not in ('success', 'fail'):
        raise ValueError("[*] O tipo deve ser 'success' ou 'fail'")

    if metrics_type == 'fail' and not error:
        raise ValueError("[*] Para tipo 'fail', o parâmetro 'error' é obrigatório")

    if metrics_type == 'success' and not metrics_data:
        raise ValueError("[*] Para tipo 'success', 'metrics_data' é obrigatório")

    ES_HOST = os.getenv("ES_HOST", "http://elasticsearch:9200")
    ES_USER = os.getenv("ES_USER")
    ES_PASS = os.getenv("ES_PASS")

    if not all([ES_USER, ES_PASS]):
        raise ValueError("[*] Credenciais do Elasticsearch não configuradas")

    if metrics_type == 'fail':
        try:
            segmentos_unicos = [row["segmento"] for row in df.select("segmento").distinct().collect()] if df else ["UNKNOWN_CLIENT"]
        except Exception:
            logger.warning("[*] Não foi possível extrair segmentos. Usando 'UNKNOWN_CLIENT'.")
            segmentos_unicos = ["UNKNOWN_CLIENT"]

        document = {
            "timestamp": datetime.now().isoformat(),
            "layer": "silver",
            "project": "compass",
            "job": "mongodb_reviews",
            "priority": "0",
            "tower": "SBBR_COMPASS",
            "client": segmentos_unicos,
            "error": str(error) if error else "Erro desconhecido"
        }
    else:
        if isinstance(metrics_data, str):
            try:
                document = json.loads(metrics_data)
            except json.JSONDecodeError as e:
                raise ValueError("[*] metrics_data não é um JSON válido") from e
        else:
            document = metrics_data

    try:
        es = Elasticsearch(
            hosts=[ES_HOST],
            basic_auth=(ES_USER, ES_PASS),
            request_timeout=30
        )

        response = es.index(
            index=index,
            document=document
        )

        logger.info(f"[*] Métricas salvas com sucesso no índice {index}. ID: {response['_id']}")
        return response

    except Exception as es_error:
        logger.error(f"[*] Falha ao salvar no Elasticsearch: {str(es_error)}")
        raise
    except Exception as e:
        logger.error(f"[*] Erro inesperado: {str(e)}")
        raise