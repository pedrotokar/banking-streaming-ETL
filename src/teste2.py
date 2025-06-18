from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
import pyspark.sql.functions as F


transactions_csv = "data/transactions"
users_csv = "data/informacoes_cadastro_100k.csv"
regions_csv = "data/regioes_estados_brasil.csv"

spark = SparkSession.builder.appName("bankingETL").getOrCreate()

schema = StructType()\
    .add("id_transacao", "string")\
    .add("id_usuario_pagador", "string")\
    .add("id_usuario_recebedor", "string")\
    .add("id_regiao_t", "string")\
    .add("modalidade_pagamento", "string")\
    .add("data_horario", "timestamp")\
    .add("valor_transacao", "double")

streaming_transactions = spark.readStream.schema(schema).csv(
    transactions_csv,
    header = True
)

streaming_output = streaming_transactions.withColumn(
    "dummy",
    F.col("valor_transacao") * 0
)

query = (streaming_output
         .writeStream.outputMode("append")
         .format("csv")
         .option("path", "data/output")
         .option("checkpointLocation", "/tmp/spark_checkpoint")
         .start())

query.awaitTermination()

spark.stop()

