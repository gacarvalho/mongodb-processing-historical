from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

def mongodb_schema_silver():
    return StructType([
        StructField('id', StringType(), True),
        StructField('customer_id', StringType(), True),
        StructField('cpf', StringType(), True),
        StructField('app', StringType(), True),
        StructField('rating', StringType(), True),
        StructField('timestamp', StringType(), True),
        StructField('comment', StringType(), True),
        StructField('app_version', StringType(), True),
        StructField('os_version', StringType(), True),
        StructField('os', StringType(), True),
        StructField('historical_data', ArrayType(ArrayType(StructType([
            StructField('customer_id', StringType(), True),
            StructField('cpf', StringType(), True),
            StructField('app', StringType(), True),
            StructField('comment', StringType(), True),
            StructField('rating', StringType(), True),
            StructField('timestamp', StringType(), True),
            StructField('app_version', StringType(), True),
            StructField('os_version', StringType(), True),
            StructField('os', StringType(), True)
        ]), True), True), True)
    ])