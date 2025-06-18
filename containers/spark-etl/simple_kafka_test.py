# containers/spark-etl/simple_kafka_test.py
# Teste simples para verificar conectividade Kafka

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

print("Iniciando teste simples do Kafka...")

# Criar SparkSession minimalista
spark = SparkSession.builder \
    .appName("KafkaTest") \
    .config("spark.sql.streaming.kafka.useDeprecatedOffsetFetching", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

try:
    print("Tentando conectar ao Kafka...")
    
    # Teste básico de conexão
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "broker:29092") \
        .option("subscribe", "bank_transactions") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    print("Conexão Kafka estabelecida!")
    
    # Apenas mostrar as mensagens brutas
    query = df \
        .select(
            F.col("key").cast("string"),
            F.col("value").cast("string"),
            F.col("topic"),
            F.col("partition"),
            F.col("offset"),
            F.col("timestamp")
        ) \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .trigger(processingTime='10 seconds') \
        .start()
    
    print("Query iniciada! Aguardando mensagens por 60 segundos...")
    query.awaitTermination(60)  # Aguarda 60 segundos
    query.stop()
    
except Exception as e:
    print(f"Erro: {e}")
    import traceback
    traceback.print_exc()
finally:
    print("Parando Spark...")
    spark.stop()
    print("Teste concluído!")