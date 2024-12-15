import json
import re
from datetime import datetime
from pyspark.sql.functions import col
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, when
from sparkmeasure import StageMetrics
from tools import *

class MetricsCollector:
    """
    Coleta métricas de execução de um estágio Spark.

    Attributes:
        spark (SparkSession): Referência para a SparkSession ativa.
        stage_metrics (StageMetrics): Instância da classe StageMetrics para coletar métricas de estágio.
        start_time (datetime): Data e hora de início da coleta de métricas.
    """

    def __init__(self, spark):
        self.spark = spark
        self.stage_metrics = StageMetrics(spark)
        self.start_time = None
        self.end_time = None

    def start_collection(self):
        self.stage_metrics.begin()
        self.start_time = datetime.now()

    def end_collection(self):
        self.end_time = datetime.now()
        self.stage_metrics.end()

    def parse_stage_metrics(self, stage_metrics_str: str) -> dict:
        stage_metrics = {}
        lines = stage_metrics_str.strip().split("\n")
        for line in lines:
            if line.strip() and not line.startswith("Scheduling mode"):
                key_value = re.split(r" => | = ", line.strip())
                if len(key_value) == 2:
                    key = key_value[0].strip()
                    value = key_value[1].strip()
                    stage_metrics[key] = value
        return stage_metrics
 
    def collect_metrics(self, valid_df: DataFrame, invalid_df: DataFrame, validation_results: dict, id_app) -> str:
        """
        Coleta métricas de processamento, validação e recursos utilizados.
        """
        if self.start_time is None or self.end_time is None:
            raise ValueError("O tempo de início ou término não foi definido corretamente. Verifique a execução dos métodos start_collection e end_collection.")

        total_time = self.end_time - self.start_time
        start_ts = self.start_time.strftime("%Y-%m-%d %H:%M:%S")
        end_ts = self.end_time.strftime("%Y-%m-%d %H:%M:%S")

        memory_used = self.spark.sparkContext._jvm.org.apache.spark.util.SizeEstimator.estimate(valid_df._jdf) / (1024 * 1024)
        data_nodes_count = len(self.spark.sparkContext.getConf().get("spark.executor.instances", "1").split(","))
        count_valid = valid_df.count()
        count_invalid = invalid_df.count()
        total_records = count_valid + count_invalid
        percentage_valid = (count_valid / total_records * 100) if total_records > 0 else 0.0
        stage_metrics_raw = self.stage_metrics.report()
        stage_metrics_dict = self.parse_stage_metrics(stage_metrics_raw)

        validation_metrics = {
            "duplicate_check": validation_results["duplicate_check"],
            "null_check": validation_results["null_check"],
            "type_consistency_check": validation_results["type_consistency_check"],
        }

        success_count = sum(1 for result in validation_metrics.values() if result["status"])
        error_count = len(validation_metrics) - success_count

        metrics = {
            "application_id": self.spark.sparkContext.applicationId,
            "sigla": {
                "sigla": "DT",
                "projeto": "compass",
                "cat": "silver_historical"
            },
            "valid_data": {
                "count": count_valid,
                "percentage": percentage_valid
            },
            "invalid_data": {
                "count": count_invalid,
                "percentage": (count_invalid / total_records * 100) if total_records > 0 else 0.0
            },
            "total_records": total_records,
            "total_processing_time": str(total_time),
            "memory_used": memory_used,
            "data_nodes_count": data_nodes_count,
            "stages": stage_metrics_dict,
            "validation_results": validation_metrics,
            "success_count": success_count,
            "error_count": error_count,
            "source": {
                "app": id_app,
                "search": "internal_database"
            },
            "_ts": {
                "compass_start_ts": start_ts,
                "compass_end_ts": end_ts
            }
        }

        return json.dumps(metrics)

def print_validation_results(results: dict):
    print(f"\nValidação da ingestão concluída para {results['total_records']} registros.\n")
    for check, result in results.items():
        if check != "total_records":
            status = "PASSOU" if result["status"] else "FALHOU"
            print(f"{check.replace('_', ' ').title()}: {status}")
            if result["message"]:
                print(f"  -> {result['message']}\n")

def validate_ingest(spark: SparkSession, df: DataFrame) -> tuple:
    """
    Valida um DataFrame de dados de ingestão.

    Args:
        df (DataFrame): O DataFrame a ser validado.

    Returns:
        tuple: Uma tupla contendo:
            - DataFrame de registros válidos.
            - DataFrame de registros inválidos.
            - Dicionário com os resultados da validação.

    Realiza as seguintes validações:
        - Verifica a existência de duplicatas.
        - Verifica a existência de valores nulos em colunas críticas.
        - Verifica a consistência dos tipos de dados.

    Códigos de retorno:
        200: Sucesso
        100: Nenhum valor nulo encontrado
        101: Valores nulos encontrados
        200: Nenhum registro duplicado encontrado
        201: Registros duplicados encontrados
        300: Consistência dos tipos de dados verificada com sucesso
        301: Valores não numéricos encontrados
    """

    df = processing_old_new(spark, df)

    validation_results = {
        "duplicate_check": {"message": "", "status": True, "code": 200},
        "null_check": {"message": "", "status": True, "code": 100},
        "type_consistency_check": {"message": "", "status": True, "code": 300},
        "total_records": df.count(),
    }

    # Verificação de duplicidade
    duplicates = df.groupBy("id").count().filter(col("count") > 1)
    duplicate_count = duplicates.count()
    if duplicate_count > 0:
        validation_results["duplicate_check"]["status"] = False
        validation_results["duplicate_check"]["code"] = 201
        validation_results["duplicate_check"]["message"] = f"Registros duplicados encontrados: {duplicate_count} registros com base no 'id'."
    else:
        validation_results["duplicate_check"]["status"] = True
        validation_results["duplicate_check"]["code"] = 200
        validation_results["duplicate_check"]["message"] = "Nenhum registro duplicado encontrado."

    # Verificação de valores nulos
    critical_columns = ["customer_id", "cpf", "rating", "app","timestamp","comment"]
    nulls_df = df.select([count(when(col(c).isNull(), c)).alias(c) for c in critical_columns])
    null_counts = nulls_df.collect()[0].asDict()

    null_issues = {col: count for col, count in null_counts.items() if count > 0}
    if null_issues:
        validation_results["null_check"]["status"] = False
        validation_results["null_check"]["code"] = 101
        validation_results["null_check"]["message"] = f"Valores nulos encontrados nas colunas: {null_issues}."
    else:
        validation_results["null_check"]["status"] = True
        validation_results["null_check"]["code"] = 100
        validation_results["null_check"]["message"] = "Nenhum valor nulo encontrado nas colunas criticas."

    # Consistência dos tipos de dados
    invalid_rating = df.filter(~df.rating.cast("int").isNotNull())
    if invalid_rating.count() > 0:
        validation_results["type_consistency_check"]["status"] = False
        validation_results["type_consistency_check"]["code"] = 301
        validation_results["type_consistency_check"]["message"] = "Valores não numericos encontrados na coluna 'rating'."
    else:
        validation_results["type_consistency_check"]["status"] = True
        validation_results["type_consistency_check"]["code"] = 300
        validation_results["type_consistency_check"]["message"] = "Consistencia dos tipos de dados verificada com sucesso."

    # Modificação: Remover o uso de subtract e filtrar registros válidos
    valid_records = df.filter(
        (col("customer_id").isNotNull()) &
        (col("rating").isNotNull()) &
        (col("comment").isNotNull()) &
        (col("app_version").isNotNull()) &
        (col("cpf").isNotNull()) &
        (col("app").isNotNull()) &
        (col("os").isNotNull()) &
        (col("os_version").isNotNull()) &
        (col("timestamp").isNotNull())
    )

    # Agora, gerando os registros inválidos de maneira mais direta
    invalid_records = df.filter(
        (col("customer_id").isNull()) |
        (col("rating").isNull()) |
        (col("comment").isNull()) |
        (col("app_version").isNull()) |
        (col("cpf").isNull()) |
        (col("app").isNull()) |
        (col("os").isNull()) |
        (col("os_version").isNull()) |
        (col("timestamp").isNull())
    )

    # Se houver duplicatas, adicionamos ao DataFrame de inválidos
    # Seleciona apenas a coluna "id" do duplicates
    duplicate_ids = duplicates.select("id")

    # Filtra os registros completos de df com base nos IDs duplicados
    duplicates_records = df.join(duplicate_ids, on="id", how="inner").select(
        df["id"],
        df["customer_id"],
        df["cpf"],
        df["app"],
        df["rating"],
        df["timestamp"],
        df["comment"],
        df["app_version"],
        df["os_version"],
        df["os"],
        df["historical_data"]
    )

    if duplicate_count > 0:
        invalid_records = invalid_records.union(duplicates)

    print_validation_results(validation_results)

    return valid_records, invalid_records, validation_results
