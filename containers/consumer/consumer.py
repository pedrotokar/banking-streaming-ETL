from confluent_kafka import Consumer
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
import json
import pandas as pd
import time
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, hour
import uuid
import numpy as np

# Configurações
KAFKA_BROKER = 'broker:29092'
SCHEMA_REGISTRY_URL = 'http://schema-registry:8081'
TOPIC_NAME = 'bank_transactions'
GROUP_ID = 'banking-consumer-group'
OUTPUT_DIR = '/app/data'
BATCH_SIZE = 1000

# Lista de estados
estados_uf = [
    "AC","AL","AP","AM","BA","CE","DF","ES","GO","MA","MT",
    "MS","MG","PA","PB","PR","PE","PI","RJ","RN","RS","RO",
    "RR","SC","SP","SE","TO"
]

def create_consumer():
    config = {
        'bootstrap.servers': KAFKA_BROKER,
        'schema.registry.url': SCHEMA_REGISTRY_URL,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'earliest'
    }
    return AvroConsumer(config)

def save_to_csv(messages, output_file):
    if not messages:
        return
    df = pd.DataFrame(messages)
    df.to_csv(output_file, index=False)

def process_with_spark(transactions_csv, users_csv, regions_csv):
    spark = SparkSession.builder.appName("bankingETL").getOrCreate()

    # Carrega os dados
    transactions = spark.read.load(
        transactions_csv,
        format="csv",
        header=True,
        inferSchema=True
    ).cache().withColumnRenamed("id_regiao", "id_regiao_t")

    users = spark.read.load(
        users_csv,
        format="csv",
        header=True,
        inferSchema=True
    ).cache().withColumnRenamed("id_regiao", "id_regiao_u")

    regions = spark.read.load(
        regions_csv,
        format="csv",
        header=True,
        inferSchema=True
    ).cache()

    # Joins
    transactions_users = transactions.join(
        users,
        transactions["id_usuario_pagador"] == users["id_usuario"],
        "left"
    )

    transactions_users_loc = transactions_users.join(
        regions.withColumnsRenamed({
            "latitude": "latitude_t",
            "longitude": "longitude_t",
            "id_regiao": "id_regiao_t"
        }),
        on="id_regiao_t",
        how="left"
    ).join(
        regions.withColumnsRenamed({
            "latitude": "latitude_u",
            "longitude": "longitude_u",
            "id_regiao": "id_regiao_u"
        }).select(
            col("latitude_u"), col("longitude_u"), col("id_regiao_u")
        ),
        on="id_regiao_u",
        how="left"
    )

    # Scores de risco
    score_t5 = transactions_users_loc.withColumn(
        "t5_score",
        (((col("latitude_t") - col("latitude_u"))**2) + 
         ((col("longitude_t") - col("longitude_u"))**2))**0.5
    )

    score_t6 = transactions_users_loc.withColumn(
        "t6_score",
        col("longitude_t") * 0
    )

    score_t7 = transactions_users_loc.withColumn(
        "t7_score",
        (hour(col("data_horario")) - 12)/12
    )

    # Score médio
    score_medio = transactions.join(
        score_t5.select(col("id_transacao"), col("t5_score")),
        on="id_transacao",
        how="inner"
    ).join(
        score_t6.select(col("id_transacao"), col("t6_score")),
        on="id_transacao",
        how="inner"
    ).join(
        score_t7.select(col("id_transacao"), col("t7_score")),
        on="id_transacao",
        how="inner"
    ).withColumn(
        "score_medio",
        (col("t5_score") + col("t6_score") + col("t7_score"))/3
    )

    spark.stop()

def process_messages(messages, transactions_csv, users_csv, regions_csv):
    """Process a batch of messages and calculate risk scores."""
    if not messages:
        return
    
    # Save transactions to CSV
    save_to_csv(messages, transactions_csv)
    
    # Process with Spark
    process_with_spark(transactions_csv, users_csv, regions_csv)
    
    print(f"Processed batch of {len(messages)} messages")

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    transactions_csv = os.path.join(OUTPUT_DIR, "transacoes_100k.csv")
    users_csv = os.path.join(OUTPUT_DIR, "informacoes_cadastro_100k.csv")
    regions_csv = os.path.join(OUTPUT_DIR, "regioes_estados_brasil.csv")

    consumer = create_consumer()
    consumer.subscribe([TOPIC_NAME])

    try:
        messages = []
        while True:
            try:
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    print(f"Erro no consumidor: {msg.error()}")
                    continue

                value = msg.value()
                messages.append(value)

                if len(messages) >= BATCH_SIZE:
                    process_messages(messages, transactions_csv, users_csv, regions_csv)
                    messages = []

            except SerializerError as e:
                print(f"Erro ao deserializar mensagem: {str(e)}")
            except Exception as e:
                print(f"Erro: {str(e)}")

    except KeyboardInterrupt:
        print("Encerrando consumidor...")
    finally:
        consumer.close()

if __name__ == "__main__":
    time.sleep(30)
    main()