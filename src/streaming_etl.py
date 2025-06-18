from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
import pyspark.sql.functions as F


schema_registry_url = "http://localhost:8081"

transactions_csv = "data/transactions"
users_csv = "data/informacoes_cadastro_100k.csv"
regions_csv = "data/regioes_estados_brasil.csv"

spark = SparkSession.builder.appName("bankingETL").getOrCreate()

schema = StructType()\
    .add("id_transacao", "string")\
    .add("id_usuario_pagador", "string")\
    .add("id_usuario_recebedor", "string")\
    .add("id_regiao", "string")\
    .add("modalidade_pagamento", "string")\
    .add("data_horario", "timestamp")\
    .add("valor_transacao", "double")

kafka_messages = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092,localhost:9091") \
    .option("subscribe", "bank_transactions") \
    .load()


parsed_messages = kafka_messages \
    .select(F.col("value").cast("string").alias("json_value")) \
    .withColumn("dados", F.from_json(F.col("json_value"), schema))

streaming_transactions = parsed_messages.select("dados.*")
#
# query = (streaming_transactions
#          .writeStream.outputMode("append")
#          .format("console")
#          .outputMode("append")
#          .option("truncate", "false")
#          .start())
#
# query.awaitTermination()
#
# spark.stop()

# streaming_transactions = spark.readStream.schema(schema).csv(
#     transactions_csv,
#     header = True
# )
streaming_transactions = streaming_transactions.withWatermark("data_horario", "10 minutes").withColumnRenamed("id_regiao", "id_regiao_t")
#=====================

users = spark.read.load(
    users_csv,
    format = "csv",
    header = True,
    inferSchema = True
).cache().withColumnRenamed("id_regiao", "id_regiao_u")

regions = spark.read.load(
    regions_csv,
    format = "csv",
    header = True,
    inferSchema = True
).cache()

#Faz joins iniciais
transactions_users = streaming_transactions.join(
    # users.select(F.col("id_regiao_u"), F.col("id_usuario")),
    users,
    streaming_transactions["id_usuario_pagador"] == users["id_usuario"],
    how = "left"
)
#transactions_users.show()

transactions_users_loc = transactions_users.join(
    regions.withColumnsRenamed({"latitude": "latitude_t",
                                "longitude": "longitude_t",
                                "id_regiao": "id_regiao_t"}),
    on = "id_regiao_t",
    how = "left"
).join(
    regions.withColumnsRenamed(
        {"latitude": "latitude_u",
         "longitude": "longitude_u",
         "id_regiao": "id_regiao_u"}
    ).select(
        F.col("latitude_u"), F.col("longitude_u"), F.col("id_regiao_u")
    ),
    on = "id_regiao_u",
    how = "left"
)
#transactions_users_loc.show()

#calcula scores de risco
streaming_output = transactions_users_loc.withColumn(
    "t5_score",
    (((F.col("latitude_t") - F.col("latitude_u"))**2) + ((F.col("longitude_t") - F.col("longitude_u"))**2))**0.5 #Expressão de coluna
).withColumn(
    "t6_score",
    F.col("longitude_t") * 0
).withColumn(
    "t7_score",
    (F.hour(F.col("data_horario")) - 12)/12
).withColumn(
    "score_medio",
    (F.col("t5_score") + F.col("t6_score") + F.col("t7_score"))/3
).withColumn(
    "score_aprovado",
    F.when(F.col("score_medio") > 6, False).otherwise(True)
).withColumn(
    "saldo_aprovado",
    F.when(F.col("saldo") > F.col("valor_transacao"), True)
    .otherwise(False)
).withColumn(
    "limite_aprovado",
    F.when(
        F.col("modalidade_pagamento") == "PIX",
        F.when(F.col("valor_transacao") > F.col("limite_PIX"), False).otherwise(True)
    ).when(
        F.col("modalidade_pagamento") == "TED",
        F.when(F.col("valor_transacao") > F.col("limite_TED"), False).otherwise(True)
    ).when(
        F.col("modalidade_pagamento") == "Boleto",
        F.when(F.col("valor_transacao") > F.col("limite_Boleto"), False).otherwise(True)
    ).otherwise(
        F.when(F.col("valor_transacao") > F.col("limite_DOC"), False).otherwise(True)
    )
).withColumn(
    "transacao_aprovada",
    F.col("score_aprovado") & F.col("saldo_aprovado") & F.col("limite_aprovado")
)

#transaction_approved.show()


streaming_output = streaming_output.select(
    F.col("id_transacao"),
    F.col("id_usuario_pagador"),
    F.col("id_usuario_recebedor"),
    F.col("id_regiao_t"),
    F.col("modalidade_pagamento"),
    F.col("data_horario"),
    F.col("valor_transacao"),
    F.col("transacao_aprovada")
)

query = (streaming_output
         .writeStream.outputMode("append")
         .format("csv")
         .option("path", "data/output")
         .option("checkpointLocation", "/tmp/spark_checkpoint")
         .start())

query.awaitTermination()

spark.stop()
