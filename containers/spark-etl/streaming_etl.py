# streaming_etl.py - Versão corrigida
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType
import pyspark.sql.functions as F

# Configurações de arquivos
users_csv = "data/informacoes_cadastro_100k.csv"
regions_csv = "data/regioes_estados_brasil.csv"

# Criar SparkSession com configurações específicas para Kafka
spark = SparkSession.builder \
    .appName("bankingETL") \
    .config("spark.sql.streaming.schemaInference", "true") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.streaming.kafka.useDeprecatedOffsetFetching", "false") \
    .getOrCreate()

# Configurar nível de log para reduzir verbosidade
spark.sparkContext.setLogLevel("WARN")

# Definir schema das transações de forma mais explícita
schema = StructType([
    StructField("id_transacao", StringType(), True),
    StructField("id_usuario_pagador", StringType(), True),
    StructField("id_usuario_recebedor", StringType(), True),
    StructField("id_regiao", StringType(), True),
    StructField("modalidade_pagamento", StringType(), True),
    StructField("data_horario", TimestampType(), True),
    StructField("valor_transacao", DoubleType(), True)
])

print("Iniciando leitura do stream Kafka...")

try:
    # Ler mensagens do Kafka com configurações mais específicas
    kafka_messages = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "broker:29092") \
        .option("subscribe", "bank_transactions") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .option("kafka.request.timeout.ms", "60000") \
        .option("kafka.session.timeout.ms", "30000") \
        .option("maxOffsetsPerTrigger", "1000") \
        .load()

    print("Stream Kafka iniciado com sucesso!")

    # Parse das mensagens JSON
    parsed_messages = kafka_messages \
        .select(F.col("value").cast("string").alias("json_value")) \
        .withColumn("dados", F.from_json(F.col("json_value"), schema))

    # Extrair dados das transações
    streaming_transactions = parsed_messages.select("dados.*") \
        .withWatermark("data_horario", "10 minutes") \
        .withColumnRenamed("id_regiao", "id_regiao_t")

    print("Carregando dados estáticos...")

    # Carregar dados estáticos (users e regions)
    users = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(users_csv) \
        .cache() \
        .withColumnRenamed("id_regiao", "id_regiao_u")

    regions = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(regions_csv) \
        .cache()

    print("Dados estáticos carregados!")

    # Join com dados de usuários
    transactions_users = streaming_transactions.join(
        users,
        streaming_transactions["id_usuario_pagador"] == users["id_usuario"],
        how="left"
    )

    # Preparar regiões para joins múltiplos
    regions_t = regions.select(
        F.col("id_regiao").alias("id_regiao_t"),
        F.col("latitude").alias("latitude_t"),
        F.col("longitude").alias("longitude_t")
    )

    regions_u = regions.select(
        F.col("id_regiao").alias("id_regiao_u"),
        F.col("latitude").alias("latitude_u"),
        F.col("longitude").alias("longitude_u")
    )

    # Join com dados de localização
    transactions_users_loc = transactions_users \
        .join(regions_t, on="id_regiao_t", how="left") \
        .join(regions_u, on="id_regiao_u", how="left")

    # Calcular scores de risco
    streaming_output = transactions_users_loc.withColumn(
        "t5_score",
        F.sqrt(
            F.pow(F.col("latitude_t") - F.col("latitude_u"), 2) + 
            F.pow(F.col("longitude_t") - F.col("longitude_u"), 2)
        )
    ).withColumn(
        "t6_score",
        F.lit(0.0)  # Score placeholder
    ).withColumn(
        "t7_score",
        (F.hour(F.col("data_horario")) - 12) / 12.0
    ).withColumn(
        "score_medio",
        (F.col("t5_score") + F.col("t6_score") + F.col("t7_score")) / 3.0
    ).withColumn(
        "score_aprovado",
        F.when(F.col("score_medio") > 6, False).otherwise(True)
    ).withColumn(
        "saldo_aprovado",
        F.when(F.col("saldo") > F.col("valor_transacao"), True).otherwise(False)
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

    # Selecionar colunas finais
    final_output = streaming_output.select(
        F.col("id_transacao"),
        F.col("id_usuario_pagador"),
        F.col("id_usuario_recebedor"),
        F.col("id_regiao_t"),
        F.col("modalidade_pagamento"),
        F.col("data_horario"),
        F.col("valor_transacao"),
        F.col("transacao_aprovada")
    )

    print("Iniciando stream de saída...")

    # Escrever saída
    query = final_output \
        .writeStream \
        .outputMode("append") \
        .format("json") \
        .option("path", "/app/data/output") \
        .option("checkpointLocation", "/tmp/spark_checkpoint") \
        .trigger(processingTime='30 seconds') \
        .start()

    print("Stream iniciado! Aguardando dados...")
    query.awaitTermination()

except Exception as e:
    print(f"Erro durante execução: {str(e)}")
    import traceback
    traceback.print_exc()
finally:
    print("Parando Spark...")
    spark.stop()