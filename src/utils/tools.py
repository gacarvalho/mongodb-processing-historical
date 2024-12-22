import os
import logging
import pymongo
import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import upper, udf, col
from pyspark.sql.types import StringType, StructType, StructField, IntegerType
from datetime import datetime
from pathlib import Path
from urllib.parse import quote_plus
from unidecode import unidecode
from src.schemas.schema_mongodb import mongodb_schema_silver


# Função para remover acentos
def remove_accents(s):
    return unidecode(s)

remove_accents_udf = F.udf(remove_accents, StringType())


def processing_reviews(spark: SparkSession, pathSource: str):

    logging.info(f"{datetime.now().strftime('%Y%m%d %H:%M:%S.%f')} [*] Processando o tratamento da camada historica")

    # Criar o DataFrame
    df = spark.read.parquet(pathSource)


    # Aplicando as transformações no DataFrame
    df_select = df.select(
        "id",
        "cpf",
        "customer_id",
        "age",
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

def save_reviews(reviews_df: DataFrame, directory: str):
    """
    Salva os dados do DataFrame no formato Delta no diretório especificado.

    Args:
        reviews_df (DataFrame): DataFrame PySpark contendo as avaliações.
        directory (str): Caminho do diretório onde os dados serão salvos.
    """
    try:
        # Verifica se o diretório existe e cria-o se não existir
        Path(directory).mkdir(parents=True, exist_ok=True)

        # Escrever os dados no formato Delta
        # reviews_df.write.format("delta").mode("overwrite").save(directory)
        reviews_df.write.option("compression", "snappy").mode("overwrite").parquet(directory)
        logging.info(f"[*] Dados salvos em {directory} no formato Delta")

    except Exception as e:
        logging.error(f"[*] Erro ao salvar os dados: {e}")
        exit(1)


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
            save_reviews(df, path)
        else:
            logging.warning(f"[*] Nenhum dado {label} foi encontrado!")
    except Exception as e:
        logging.error(f"[*] Erro ao salvar dados {label}: {e}", exc_info=True)
        
def write_to_mongo(dados_feedback: dict, table_id: str):

    mongo_user = os.environ["MONGO_USER"]
    mongo_pass = os.environ["MONGO_PASS"]
    mongo_host = os.environ["MONGO_HOST"]
    mongo_port = os.environ["MONGO_PORT"]
    mongo_db = os.environ["MONGO_DB"]

    # ---------------------------------------------- Escapar nome de usuário e senha ----------------------------------------------
    # A função quote_plus transforma caracteres especiais em seu equivalente escapado, de modo que o
    # URI seja aceito pelo MongoDB. Por exemplo, m@ngo será convertido para m%40ngo.
    escaped_user = quote_plus(mongo_user)
    escaped_pass = quote_plus(mongo_pass)

    # ---------------------------------------------- Conexão com MongoDB ----------------------------------------------------------
    # Quando definimos maxPoolSize=1, estamos dizendo ao MongoDB para manter apenas uma conexão aberta no pool.
    # Isso implica que cada vez que uma nova operação precisa de uma conexão, a conexão existente será
    # reutilizada em vez de criar uma nova.
    mongo_uri = f"mongodb://{escaped_user}:{escaped_pass}@{mongo_host}:{mongo_port}/{mongo_db}?authSource={mongo_db}&maxPoolSize=1"

    client = pymongo.MongoClient(mongo_uri)

    try:
        db = client[mongo_db]
        collection = db[table_id]

        # Inserir dados no MongoDB
        if isinstance(dados_feedback, dict):  # Verifica se os dados são um dicionário
            collection.insert_one(dados_feedback)
        elif isinstance(dados_feedback, list):  # Verifica se os dados são uma lista
            collection.insert_many(dados_feedback)
        else:
            print("[*] Os dados devem ser um dicionário ou uma lista de dicionários.")
    finally:
        # Garante que a conexão será fechada
        client.close()


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
            StructField("os_version", StringType(), True),
            StructField("rating", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("votes_count", StringType(), True),
            StructField("odate", StringType(), True)
        ])

        df_historical = spark.createDataFrame([], schema)

    df_historical.printSchema()
    # Definindo aliases para os DataFrames
    new_reviews_df_alias = df.alias("new")  # DataFrame de novos reviews
    historical_reviews_df_alias = df_historical.alias("old")  # DataFrame de reviews históricos

    # Junção dos DataFrames
    joined_reviews_df = new_reviews_df_alias.join(historical_reviews_df_alias, "cpf", "outer")

    # Criação da coluna historical_data
    result_df = joined_reviews_df.withColumn(
        "historical_data_temp",
        F.when(
            (F.col("new.customer_id").isNotNull()) & (F.col("old.customer_id").isNotNull()) & (F.col("new.customer_id") != F.col("old.customer_id")),
            F.array(F.struct(
                F.col("old.customer_id").alias("customer_id"),
                F.col("old.cpf").alias("cpf"),
                F.col("old.app").alias("app"),
                F.col("old.comment").alias("comment"),
                F.col("old.rating").cast("string").alias("rating"),
                F.col("old.timestamp").alias("timestamp"),
                F.col("old.app_version").alias("app_version"),
                F.col("old.os_version").alias("os_version"),
                F.col("old.os").alias("os")
            ))
        ).when(
            (F.col("new.cpf").isNotNull()) & (F.col("old.cpf").isNotNull()) & (F.col("new.cpf") != F.col("old.cpf")),
            F.array(F.struct(
                F.col("old.customer_id").alias("customer_id"),
                F.col("old.cpf").alias("cpf"),
                F.col("old.app").alias("app"),
                F.col("old.comment").alias("comment"),
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
                F.col("old.rating").cast("string").alias("rating"),
                F.col("old.timestamp").alias("timestamp"),
                F.col("old.app_version").alias("app_version"),
                F.col("old.os_version").alias("os_version"),
                F.col("old.os").alias("os")
            ))
        )
    )

    print("tests")
    result_df.printSchema()

    # Agrupando e coletando históricos
    df_final = result_df.groupBy("new.id").agg(
    F.coalesce(F.first(F.col("new.customer_id")), F.first(F.col("old.customer_id"))).alias("customer_id"),
    F.coalesce(F.first(F.col("new.cpf")), F.first(F.col("old.cpf"))).alias("cpf"),
    F.coalesce(F.first(F.col("new.app")), F.first(F.col("old.app"))).alias("app"),
    F.coalesce(F.first(F.col("new.rating")), F.first(F.col("old.rating"))).alias("rating"),
    F.coalesce(F.first(F.col("new.timestamp")), F.first(F.col("old.timestamp"))).alias("timestamp"),
    F.coalesce(F.first(F.col("new.comment")), F.first(F.col("old.comment"))).alias("comment"),
    F.coalesce(F.first(F.col("new.app_version")), F.first(F.col("old.app_version"))).alias("app_version"),
    F.coalesce(F.first(F.col("new.os_version")), F.first(F.col("old.os_version"))).alias("os_version"),
    F.coalesce(F.first(F.col("new.os")), F.first(F.col("old.os"))).alias("os"),
    F.collect_list("historical_data_temp").alias("historical_data"),
)


    # Exibe o resultado
    df_final.show(truncate=False)

    return df_final