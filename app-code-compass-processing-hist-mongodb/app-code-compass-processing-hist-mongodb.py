import logging
import sys
import json
from pyspark.sql import SparkSession
from metrics import MetricsCollector, validate_ingest
from datetime import datetime
from tools import *

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():

    # Capturar argumentos da linha de comando
    args = sys.argv
    print("[*] ARGUMENTOS: " + str(args))

    try:
        # Criação da sessão Spark
        with spark_session() as spark:

            # Coleta de métricas
            metrics_collector = MetricsCollector(spark)
            metrics_collector.start_collection()

            datePath = datetime.now().strftime("%Y%m%d")

            # Definindo caminhos
            pathSource = f"/santander/bronze/compass/reviews/mongodb/*/odate={datePath}/"
            path_target = f"/santander/silver/compass/reviews/mongodb/odate={datePath}/"
            path_target_fail = f"/santander/silver/compass/reviews_fail/mongodb/odate={datePath}/"

            df_processado = processing_reviews(spark, pathSource)  # Função presumida; ajuste conforme necessário
            
            

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
    Salva as métricas no MongoDB.
    """
    try:
        metrics_data = json.loads(metrics_json)
        write_to_mongo(metrics_data, "dt_datametrics_compass")
        logging.info(f"[*] Métricas da aplicação salvas: {metrics_json}")
    except json.JSONDecodeError as e:
        logging.error(f"[*] Erro ao processar métricas: {e}", exc_info=True)

if __name__ == "__main__":
    main()
