import os
import sys
import json
import logging
from datetime import datetime
from typing import Tuple
from pyspark.sql import SparkSession, DataFrame
try:
    from tools import *
    from metrics import MetricsCollector, validate_ingest
except ModuleNotFoundError:
    from src.utils.tools import *
    from src.metrics.metrics import MetricsCollector, validate_ingest

# Configuração centralizada
FORMAT = "parquet"
PATH_BRONZE_BASE = "/santander/bronze/compass/reviews/mongodb/"
PATH_SILVER_BASE = "/santander/silver/compass/reviews/mongodb/"
PATH_SILVER_FAIL_BASE = "/santander/silver/compass/reviews_fail/mongodb/"
PARTITION_COLUMN = "odate"
COMPRESSION_TYPE = "snappy"
ENV_PRE_VALUE = "pre"
ELASTIC_INDEX_SUCCESS = "compass_dt_datametrics"
ELASTIC_INDEX_FAIL = "compass_dt_datametrics_fail"


# Logging estruturado
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(message)s"
)

logger = logging.getLogger(__name__)


class PipelineConfig:
    def __init__(self, env: str):
        self.env = env
        self.date_str = datetime.now().strftime("%Y%m%d")
        self.path_source_pf = f"{PATH_BRONZE_BASE}*_pf/odate={self.date_str}/"
        self.path_source_pj = f"{PATH_BRONZE_BASE}*_pj/odate={self.date_str}/"
        self.path_target = f"{PATH_SILVER_BASE}"
        self.path_target_fail = f"{PATH_SILVER_FAIL_BASE}"


def create_spark_session() -> SparkSession:
    try:
        spark = SparkSession.builder \
            .appName("App Reviews [mongodb internal-database]") \
            .config("spark.jars.packages", "org.apache.spark:spark-measure_2.12:0.16") \
            .config("spark.sql.parquet.enableVectorizedReader", "false") \
            .getOrCreate()
        logger.info("[*] Spark Session criada com sucesso.")
        return spark
    except Exception as e:
        logger.error("[*] Falha ao criar SparkSession", exc_info=True)
        save_metrics(
            metrics_type="fail",
            index=ELASTIC_INDEX_FAIL,
            error=e
        )
        raise


def read_and_union_data(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    logger.info("[*] Lendo dados de origem dos caminhos PF e PJ.")
    try:
        df_pf = read_source_parquet(spark, config.path_source_pf)
        df_pj = read_source_parquet(spark, config.path_source_pj)

        dfs = [df for df in [df_pf, df_pj] if df is not None]

        if not dfs:
            logger.warning("[*] Nenhum dado encontrado - criando DataFrame vazio.")
            empty_schema = spark.read.parquet(config.path_source_pf).schema
            return spark.createDataFrame([], schema=empty_schema)

        return dfs[0] if len(dfs) == 1 else dfs[0].unionByName(dfs[1])
    except Exception as e:
        logger.error("[*] Erro ao ler e unir dados", exc_info=True)
        save_metrics(
            metrics_type="fail",
            index=ELASTIC_INDEX_FAIL,
            error=e
        )
        raise


def process_and_validate_data(spark: SparkSession, df: DataFrame, config: PipelineConfig) -> Tuple[DataFrame, DataFrame, dict]:
    try:
        logger.info("[*] Processando dados de avaliações")
        df_processed = processing_reviews(df)

        logger.info("[*] Validando a qualidade dos dados")
        valid_df, invalid_df, validation_results = validate_ingest(spark, df_processed)

        if config.env == ENV_PRE_VALUE:
            logger.info("[*] Dados de amostra: valid_df")
            valid_df.show(10, truncate=False)
            logger.info("[*] Dados de amostra: invalid_df")
            invalid_df.show(10, truncate=False)

        return valid_df, invalid_df, validation_results
    except Exception as e:
        logger.error("[*] Erro no processamento/validação", exc_info=True)
        save_metrics(
            metrics_type="fail",
            index=ELASTIC_INDEX_FAIL,
            error=e
        )
        raise


def save_output_data(valid_df: DataFrame, invalid_df: DataFrame, config: PipelineConfig) -> None:
    try:
        logger.info(f"[*] Salvando dados válidos em {config.path_target}")
        save_dataframe(
            df=valid_df,
            path=config.path_target,
            label="valido",
            partition_column=PARTITION_COLUMN,
            compression=COMPRESSION_TYPE
        )

        logger.info(f"[*] Salvando dados inválidos em {config.path_target_fail}")
        save_dataframe(
            df=invalid_df,
            path=config.path_target_fail,
            label="invalido",
            partition_column=PARTITION_COLUMN,
            compression=COMPRESSION_TYPE
        )
    except Exception as e:
        logger.error("[*] Falha ao salvar dados", exc_info=True)
        save_metrics(
            metrics_type="fail",
            index=ELASTIC_INDEX_FAIL,
            error=e
        )
        raise



def execute_pipeline(spark: SparkSession, config: PipelineConfig) -> None:
    try:
        metrics_collector = MetricsCollector(spark)
        metrics_collector.start_collection()

        df = read_and_union_data(spark, config)
        valid_df, invalid_df, validation_results = process_and_validate_data(spark, df, config)
        save_output_data(valid_df, invalid_df, config)

        metrics_collector.end_collection()

        metrics_json = metrics_collector.collect_metrics(valid_df, invalid_df, validation_results, "silver_internal_database")

        save_metrics(
            metrics_type='success',
            index=ELASTIC_INDEX_SUCCESS,
            df=valid_df,
            metrics_data=metrics_json
        )

        logger.info(f"[*] Métricas coletadas: {metrics_json}")
        logger.info("[*] Pipeline executado com sucesso.")
    except Exception as e:
        logger.error("[*] Falha na execução do pipeline", exc_info=True)
        save_metrics(
            metrics_type="fail",
            index=ELASTIC_INDEX_FAIL,
            error=e
        )
        raise


def main():
    if len(sys.argv) != 2:
        logger.error("Uso: spark-submit app.py <env>")
        sys.exit(1)

    env = sys.argv[1]
    config = PipelineConfig(env)
    spark = None

    try:
        spark = create_spark_session()
        execute_pipeline(spark, config)
    except Exception as e:
        logger.error("[*] Pipeline falhou de forma crítica", exc_info=True)
        save_metrics(
            metrics_type="fail",
            index=ELASTIC_INDEX_FAIL,
            error=e
        )
        sys.exit(1)
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    main()
