from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType
import pyspark.sql.functions as F
import redis
import os

users_csv = "data/informacoes_cadastro_100k.csv"
regions_csv = "data/regioes_estados_brasil.csv"

spark = SparkSession.builder \
    .appName("bankingETL") \
    .config("spark.jars.packages", "com.redislabs:spark-redis_2.12:3.0.0,org.postgresql:postgresql:42.6.0") \
    .config("spark.sql.streaming.schemaInference", "true") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.streaming.kafka.useDeprecatedOffsetFetching", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

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

    # Capturar timestamp de chegada da mensagem no Kafka (tempo de entrada)
    parsed_messages = kafka_messages \
        .select(
            F.col("value").cast("string").alias("json_value"),
            F.col("timestamp").alias("kafka_timestamp")  # Timestamp do Kafka
        ) \
        .withColumn("dados", F.from_json(F.col("json_value"), schema))

    # Adicionar timestamp de início do processamento
    streaming_transactions = parsed_messages.select("dados.*", "kafka_timestamp") \
        .withColumn("tempo_inicio_processamento", F.current_timestamp()) \
        .withWatermark("data_horario", "10 minutes") \
        .withColumnRenamed("id_regiao", "id_regiao_t")

    print("Carregando dados estáticos...")

    jdbc_link = "jdbc:postgresql://postgres:5432/bank?stringtype=unspecified"
    connection_info = {
        "user": "bank_etl",
        "password": "ihateavroformat123",
        "driver": "org.postgresql.Driver"
    }

    users = spark.read.jdbc(
        url = jdbc_link,
        table = "usuarios",
        properties = connection_info
    ).cache()

    users = users.withColumnRenamed("id_regiao", "id_regiao_u")

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

    # Selecionar colunas finais com métricas de tempo
    final_output = streaming_output.select(
        F.col("id_transacao"),
        F.col("id_usuario_pagador"),
        F.col("id_usuario_recebedor"),
        F.col("id_regiao_t"),
        F.col("modalidade_pagamento"),
        F.col("data_horario"),
        F.col("valor_transacao"),
        F.col("transacao_aprovada"),
        # Adicionar as métricas de tempo
        F.current_timestamp().alias("tempo_saida_resultado"),  # Timestamp de saída
        F.col("kafka_timestamp").alias("tempo_entrada_kafka"),  # Timestamp de entrada no Kafka
        F.col("tempo_inicio_processamento"),  # Timestamp início processamento
        # Calcular diferenças de tempo em milissegundos
        (F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp(F.col("kafka_timestamp"))).alias("latencia_total_ms"),
        (F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp(F.col("tempo_inicio_processamento"))).alias("tempo_processamento_ms")
    ).withColumnRenamed("id_regiao_t", "id_regiao")

    print("Iniciando stream de saída...")

    def write_microbatch_to_sinks(data, mbatch_id):
        data.persist()

        data.write \
            .format("jdbc") \
            .option("url", jdbc_link) \
            .option("dbtable", "transacoes") \
            .option("user", connection_info["user"]) \
            .option("password", connection_info["password"]) \
            .option("driver", connection_info["driver"]) \
            .mode("append") \
            .save()
        print(f"Micro-batch {mbatch_id} written to PostgreSQL.")

        # Write to Redis
        print(f"Writing micro-batch {mbatch_id} to Redis...")
        data.write \
            .format("org.apache.spark.sql.redis") \
            .option("host", os.getenv("REDIS_HOST", "redis")) \
            .option("port", os.getenv("REDIS_PORT", "6379")) \
            .option("table", "transacoes") \
            .option("key.column", "id_transacao") \
            .mode("append") \
            .save()
        print(f"Micro-batch {mbatch_id} written to Redis.")

        # Adiciona os IDs das transações a um Sorted Set para a funcionalidade de 'itens mais recentes'
        print(f"Adding to Sorted Set in Redis for micro-batch {mbatch_id}...")

        def add_to_sorted_set(partition):
            # precisa criar aqui dentro pq
            # o objeto do cliente não é serializável
            redis_client = redis.Redis(
                host=os.getenv("REDIS_HOST", "redis"),
                port=int(os.getenv("REDIS_PORT", "6379")),
                db=0
            )

            pipeline = redis_client.pipeline()
            sorted_set_key = "recent_transactions"
            for row in partition:
                transaction_id = row["id_transacao"]
                timestamp_score = row["data_horario"].timestamp()
                pipeline.zadd(sorted_set_key, {transaction_id: timestamp_score})
            pipeline.execute()

        data.select("id_transacao", "data_horario").rdd.foreachPartition(add_to_sorted_set)
        print(f"Micro-batch {mbatch_id} added to Redis Sorted Set.")

        data.unpersist()

    query = final_output \
        .writeStream \
        .foreachBatch(write_microbatch_to_sinks) \
        .outputMode("append") \
        .option("checkpointLocation", "/tmp/spark_checkpoint") \
        .start()

    # Stream adicional para console com métricas de latência
    metrics_query = final_output.select(
        "id_transacao",
        "transacao_aprovada", 
        "tempo_saida_resultado",
        "latencia_total_ms",
        "tempo_processamento_ms"
    ).writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", "5") \
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