import os
import sys
import json
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, regexp_extract
from datetime import datetime
from elasticsearch import Elasticsearch
try:
    # Obtem import para cenarios de execuções em ambiente PRE, PRD
    from tools import *
    from metrics import MetricsCollector, validate_ingest
except ModuleNotFoundError:
    # Obtem import para cenarios de testes unitarios
    from src.utils.tools import *
    from src.metrics.metrics import MetricsCollector, validate_ingest


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():

    # Capturar argumentos da linha de comando
    args = sys.argv

    # Verificar se o número correto de argumentos foi passado
    if len(args) != 2:
        print("[*] Usage: spark-submit app.py <env> ")
        sys.exit(1)

    # Criação da sessão Spark
    spark = spark_session()

    try:

        # Entrada e captura de variaveis e parametros
        env = args[1]

        # Coleta de métricas
        metrics_collector = MetricsCollector(spark)
        metrics_collector.start_collection()

        # Definindo caminhos
        datePath = datetime.now().strftime("%Y%m%d")

        # Definindo caminhos
        pathSource_pf = f"/santander/bronze/compass/reviews/mongodb/*_pf/odate={datePath}/"
        pathSource_pj = f"/santander/bronze/compass/reviews/mongodb/*_pj/odate={datePath}/"
        path_target = f"/santander/silver/compass/reviews/mongodb/odate={datePath}/"
        path_target_fail = f"/santander/silver/compass/reviews_fail/mongodb/odate={datePath}/"

        # Leitura do arquivo Parquet
        logging.info(f"[*] Iniciando leitura dos path origens.", exc_info=True)
        df_pf = read_source_parquet(spark, pathSource_pf)
        df_pj = read_source_parquet(spark, pathSource_pj)

        # Mantém apenas os DataFrames que possuem dados
        dfs = [df for df in [df_pf, df_pj] if df is not None]

        # Se houver pelo menos um DataFrame com dados, une os resultados
        if dfs:
            df = dfs[0] if len(dfs) == 1 else dfs[0].unionByName(dfs[1])
        else:
            print("[*] Nenhum dado encontrado! Criando DataFrame vazio...")
            empty_schema = spark.read.parquet(pathSource_pf).schema
            df = spark.createDataFrame([], schema=empty_schema)


        df_processado = processing_reviews(df)


        #Valida o DataFrame e coleta resultados
        valid_df, invalid_df, validation_results = validate_ingest(spark, df_processado)

        valid_df.printSchema()
        valid_df.show(valid_df.count(), truncate=False)

        # Salvar dados válidos
        save_dataframe(valid_df, path_target, "valido")

        # Salvar dados inválidos
        save_dataframe(invalid_df, path_target_fail, "invalido")

        # Coleta métricas após o processamento
        metrics_collector.end_collection()
        metrics_json = metrics_collector.collect_metrics(valid_df, invalid_df, validation_results, "silver_internal_database")
        # Salvar métricas no MongoDB
        save_metrics(metrics_json)

    except Exception as e:
        logging.error(f"[*] An error occurred: {e}", exc_info=True)

        # JSON de erro
        error_metrics = {
            "timestamp": datetime.now().isoformat(),
            "layer": "silver",
            "project": "compass",
            "job": "apple_store_reviews",
            "priority": "0",
            "tower": "SBBR_COMPASS",
            "client": "[NA]",
            "error": str(e)
        }

        metrics_json = json.dumps(error_metrics)

        # Salvar métricas de erro no MongoDB
        save_metrics_job_fail(metrics_json)

    finally:
        spark.stop()

def spark_session():
    try:
        spark = SparkSession.builder \
            .appName("App Reviews [mongodb internal-database]") \
            .config("spark.jars.packages", "org.apache.spark:spark-measure_2.12:0.16") \
            .config("spark.sql.parquet.enableVectorizedReader", "false") \
            .getOrCreate()
        return spark
    except Exception as e:
        logging.error(f"[*] Failed to create SparkSession: {e}", exc_info=True)
        raise


def save_metrics(metrics_json):
    """
    Salva as métricas.
    """

    ES_HOST = "http://elasticsearch:9200"
    ES_INDEX = "compass_dt_datametrics"
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

        # JSON de erro
        error_metrics = {
            "timestamp": datetime.now().isoformat(),
            "layer": "silver",
            "project": "compass",
            "job": "apple_store_reviews",
            "priority": "3",
            "tower": "SBBR_COMPASS",
            "client": "[NA]",
            "error": str(e)
        }

        metrics_json = json.dumps(error_metrics)

        # Salvar métricas de erro no MongoDB
        save_metrics_job_fail(metrics_json)

    except Exception as e:
        logging.error(f"[*] Erro ao salvar métricas no Elasticsearch: {e}", exc_info=True)

if __name__ == "__main__":
    main()
