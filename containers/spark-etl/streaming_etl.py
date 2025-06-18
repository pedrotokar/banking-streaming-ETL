from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, DoubleType, LongType
import pyspark.sql.functions as F

spark = SparkSession.builder \
    .appName("KafkaBankingETL") \
    .getOrCreate()

df_kafka = spark.readStream.format("kafka")\
    .option("kafka.bootstrap.servers", "broker:29092")\
    .option("subscribe", "bank_transactions")\
    .option("startingOffsets", "latest")\
    .load()

df_json = df_kafka.selectExpr("CAST(value AS STRING) as json_str")

df_json.writeStream.format("console").start().awaitTermination()

schema = StructType()\
    .add("id_transacao", StringType())\
    .add("id_usuario_pagador", StringType())\
    .add("id_usuario_recebedor", StringType())\
    .add("id_regiao", StringType())\
    .add("modalidade_pagamento", StringType())\
    .add("data_horario", LongType())\
    .add("valor_transacao", DoubleType())\
    .add("saldo_pagador", DoubleType())\
    .add("limite_modalidade", DoubleType())\
    .add("latitude", DoubleType())\
    .add("longitude", DoubleType())\
    .add("media_transacional_mensal", DoubleType())\
    .add("num_fraudes_ult_30d", StringType())

df_structured = df_json.select(F.from_json(F.col("json_str"), schema).alias("data")).select("data.*")

df_result = df_structured.withColumn("data_horario", (F.col("data_horario") / 1000).cast("timestamp"))\
    .withColumn("score_risco", 
        F.sqrt((F.col("latitude") - 0)**2 + (F.col("longitude") - 0)**2)
    )\
    .withColumn("score_aprovado", F.when(F.col("score_risco") < 50, True).otherwise(False))

query = df_result.select("id_transacao", "modalidade_pagamento", "valor_transacao", "score_aprovado")\
    .writeStream.outputMode("append")\
    .format("console")\
    .option("truncate", False)\
    .start()

query.awaitTermination()
