import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout
import pyspark.sql.functions as F
import redis
import os

print(f"======Iniciando PySpark versão {pyspark.__version__}=======")

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
spark.conf.set("spark.sql.streaming.stateStore.providerClass", "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")

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

state_output_schema = StructType([
    StructField("id_transacao", StringType(), True),
    StructField("id_usuario_pagador", StringType(), True),
    StructField("id_usuario_recebedor", StringType(), True),
    StructField("id_regiao_t", StringType(), True),
    StructField("modalidade_pagamento", StringType(), True),
    StructField("data_horario", TimestampType(), True),
    StructField("valor_transacao", DoubleType(), True),
    StructField("kafka_timestamp", TimestampType(), True),
    StructField("tempo_inicio_processamento", TimestampType(), True),
    StructField("id_usuario", StringType(), True),
    StructField("id_regiao_u", StringType(), True),
    StructField("saldo", DoubleType(), True),
    StructField("limite_pix", DoubleType(), True),
    StructField("limite_ted", DoubleType(), True),
    StructField("limite_doc", DoubleType(), True),
    StructField("limite_boleto", DoubleType(), True),
    StructField("data_ultima_transacao", TimestampType(), True),
    StructField("created_at", TimestampType(), True)
])

state_schema = StructType([
    StructField("data_ultima_transacao", TimestampType(), True)
])

print("Iniciando leitura do stream Kafka...")

#=========
def state_function(key, df_iter, state: GroupState):
    """Essa função vai garantir que não vamos processar transações que chegaram
    em microbatches atrasados depois de uma transacao mais recente ser processada.
    Isso garante indempotencia, já que se o Spark Structured Streaming mandar a
    mesma transacao de novo, ela não será processada."""

    user_id = key[0]
#    print("usuario id", user_id)
    if state.hasTimedOut:
        print(f"Estado para o usuário {user_id} expirou e foi removido.")
        state.remove()
        return # Não retorna nada

    if not state.exists:
        state_timestamp = None #vai pegar da linha
        most_recent_timestamp = None
    else:
        state_timestamp = state.get[0] #state atual pra não ficar dando get
        most_recent_timestamp = state.get[0] #qual vai ser a transacao mais recente do microbatch

    for df in df_iter:
        if state_timestamp is None:
            state_timestamp = df.loc[0, "data_ultima_transacao"]
            most_recent_timestamp = df.loc[0, "data_ultima_transacao"]
#           print(df["data_horario"], state_timestamp, (df["data_horario"] >= state_timestamp).sum())
        df = df.loc[df["data_horario"] >= state_timestamp]
        if not df.empty:
#           print(df.columns)
            most_recent_timestamp = max(most_recent_timestamp, df["data_horario"].max())
            yield df

    if most_recent_timestamp > state_timestamp:
        state.update((most_recent_timestamp,))
    state.setTimeoutDuration(60000)
#=========

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
        .load()
        # .option("maxOffsetsPerTrigger", "1000") \

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
    ).withColumn("saldo", F.col("saldo").astype("double")) \
    .withColumn("limite_pix", F.col("limite_pix").astype("double")) \
    .withColumn("limite_ted", F.col("limite_ted").astype("double")) \
    .withColumn("limite_doc", F.col("limite_doc").astype("double")) \
    .withColumn("limite_boleto", F.col("limite_boleto").astype("double")) \
    .cache()

    users = users.withColumnRenamed("id_regiao", "id_regiao_u")

    regions = spark.read.jdbc(
        url = jdbc_link,
        table = "regioes",
        properties = connection_info
    ).cache()

    print("Dados estáticos carregados!")

    # Join com dados de usuários
    transactions_users = streaming_transactions.join(
        users,
        streaming_transactions["id_usuario_pagador"] == users["id_usuario"],
        how="left"
    )
    transactions_users = transactions_users.groupBy("id_usuario_pagador").applyInPandasWithState(
        state_function,
        state_output_schema,
        state_schema,
        "append",
        GroupStateTimeout.ProcessingTimeTimeout
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
        (F.col("valor_transacao") > 500).astype("double")
    ).withColumn(
        "t7_score",
        (F.hour(F.col("data_horario")) - 12) / 12.0
    ).withColumn(
        "score_medio",
        (F.col("t5_score") * F.col("t6_score") * F.col("t7_score")) / 3.0
    ).withColumn(
        "score_aprovado",
        F.when(F.col("score_medio") > 6, False).otherwise(True)
    ).withColumn(
        "limite_aprovado",
        F.when(
            F.col("modalidade_pagamento") == "PIX",
            F.col("valor_transacao") < F.col("limite_PIX")
        ).when(
            F.col("modalidade_pagamento") == "TED",
            F.col("valor_transacao") < F.col("limite_TED")
        ).when(
            F.col("modalidade_pagamento") == "Boleto",
            F.col("valor_transacao") < F.col("limite_Boleto")
        ).otherwise(
            F.col("valor_transacao") < F.col("limite_DOC")
        )
    ).withColumn(
        "saldo_aprovado",
        F.when(F.col("saldo") > F.col("valor_transacao"), True).otherwise(False)
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
        F.col("t5_score"),
        F.col("t6_score"),
        F.col("t7_score"),
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

        updates_horarios = data.groupBy("id_usuario_pagador").agg(
            F.max("data_horario").alias("data_ultima_transacao")
        ).withColumnRenamed("id_usuario_pagador", "id_usuario")


        transacoes_df = data.select(
            "id_transacao",
            "id_usuario_pagador",
            "id_usuario_recebedor",
            "id_regiao",
            "modalidade_pagamento",
            "data_horario",
            "valor_transacao",
            "transacao_aprovada",
            "tempo_saida_resultado",
            "tempo_entrada_kafka",
            "tempo_inicio_processamento",
            "latencia_total_ms",
            "tempo_processamento_ms"
        )

        updates_horarios.write \
            .format("jdbc") \
            .option("url", jdbc_link) \
            .option("dbtable", "staging_updates_usuarios") \
            .option("user", connection_info["user"]) \
            .option("password", connection_info["password"]) \
            .option("driver", connection_info["driver"]) \
            .mode("overwrite") \
            .save()

        transacoes_df.write \
            .format("jdbc") \
            .option("url", jdbc_link) \
            .option("dbtable", "transacoes") \
            .option("user", connection_info["user"]) \
            .option("password", connection_info["password"]) \
            .option("driver", connection_info["driver"]) \
            .mode("append") \
            .save()
        print(f"Micro-batch {mbatch_id} written to PostgreSQL (transacoes).")

        scores_df = data.select("id_transacao", "t5_score", "t6_score", "t7_score")
        scores_df.write \
            .format("jdbc") \
            .option("url", jdbc_link) \
            .option("dbtable", "transacoes_scores") \
            .option("user", connection_info["user"]) \
            .option("password", connection_info["password"]) \
            .option("driver", connection_info["driver"]) \
            .mode("append") \
            .save()
        print(f"Micro-batch {mbatch_id} written to PostgreSQL (transacoes_scores).")

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
                timestamp_score = row["tempo_saida_resultado"].timestamp()
                pipeline.zadd(sorted_set_key, {transaction_id: timestamp_score})
            pipeline.execute()

        data.select("id_transacao", "tempo_saida_resultado").rdd.foreachPartition(add_to_sorted_set)
        print(f"Micro-batch {mbatch_id} added to Redis Sorted Set.")

        data.unpersist()

    query = final_output \
        .writeStream \
        .foreachBatch(write_microbatch_to_sinks) \
        .outputMode("append") \
        .option("checkpointLocation", "/tmp/spark_checkpoint") \
        .trigger(processingTime='1 seconds') \
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
