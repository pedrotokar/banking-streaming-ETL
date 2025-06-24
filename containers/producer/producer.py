import numpy as np
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
import psycopg2
import psycopg2.extras

from datetime import datetime, timedelta
import os
import json
import time
import uuid

KAFKA_BROKER = 'broker:29092'
TOPIC_NAME = 'bank_transactions'

conn_params = {
    "host": "postgres",
    "port": "5432",
    "dbname": "bank",
    "user": "bank_etl",
    "password": "ihateavroformat123"
}

producer_config = {
    'bootstrap.servers': KAFKA_BROKER,
    'client.id': 'bank-transaction-producer'
}

np.random.seed(42)
payment_methods = ["PIX", "TED", "DOC", "Boleto"]
estados_uf = [
    "AC","AL","AP","AM","BA","CE","DF","ES","GO","MA","MT",
    "MS","MG","PA","PB","PR","PE","PI","RJ","RN","RS","RO",
    "RR","SC","SP","SE","TO"
]

same_limit_for_all = True

region_list = []
regioes = {}
for uf in estados_uf:
    lat = np.round(-34 + np.random.rand() * 39, 6)
    lon = np.round(-74 + np.random.rand() * 40, 6)
    media_mens = np.round(1_000 + np.random.rand() * 30_000, 2)
    num_fraude = int(np.random.randint(0, 100))
    region_list.append([uf, lat, lon, media_mens, num_fraude])
    regioes[uf] = {
        "latitude": lat,
        "longitude": lon,
        "media_transacional_mensal": media_mens,
        "num_fraudes_ult_30d": num_fraude
    }


def create_topic(topic_name, num_partitions=1, replication_factor=1):
    """Create Kafka topic if it doesn't exist."""
    admin_client = AdminClient({'bootstrap.servers': KAFKA_BROKER})
    
    topics = admin_client.list_topics().topics
    if topic_name not in topics:
        print(f"Creating topic {topic_name}")
        topic = NewTopic(
            topic_name,
            num_partitions=num_partitions,
            replication_factor=replication_factor
        )
        admin_client.create_topics([topic])
        print(f"Topic {topic_name} created")
    else:
        print(f"Topic {topic_name} already exists")

def delivery_report(err, msg):
    """Callback invoked on message delivery success or failure."""
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}] @ {msg.offset()}')


def generate_transaction(usuarios):
    """Generate a single random transaction."""
    selecao = np.random.choice(np.arange(len(usuarios)), 2, replace = False)
    pagador = usuarios[selecao[0]]
    recebedor = usuarios[selecao[1]]
    valor = round(np.random.exponential(scale=1000), 2)
    
    if valor > pagador[1]:
        valor = round(float(pagador[1]) * np.random.random(), 2)
    
    modalidade = np.random.choice(payment_methods)
    
    # limite_key = f"limite_{modalidade}"
    # if valor > pagador[limite_key]:
    #     valor = round(pagador[limite_key] * np.random.random(), 2)
    
    data_horario = (datetime.now() - timedelta(
        seconds=int(np.random.rand() * 86400 * 365)
    )).isoformat()
    
    transaction = {
        'id_transacao': str(uuid.uuid4()),
        'id_usuario_pagador': pagador[0],
        'id_usuario_recebedor': recebedor[0],
        'id_regiao': np.random.choice(estados_uf),
        'modalidade_pagamento': modalidade,
        'data_horario': data_horario,
        'valor_transacao': valor,
    }

    # 'saldo_pagador': pagador["saldo"],
    # 'limite_modalidade': pagador[limite_key],
    # 'latitude': pagador["latitude"],
    # 'longitude': pagador["longitude"],
    # 'media_transacional_mensal': pagador["media_transacional_mensal"],
    # 'num_fraudes_ult_30d': pagador["num_fraudes_ult_30d"]

    return transaction

def main():
    print("Will now fetch users from postgres...")

    try:
        print("Connecting to postgres...")
        with psycopg2.connect(**conn_params) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id_usuario, saldo FROM usuarios;")

                usuarios = cur.fetchall()
        print(f"Fetched existing {len(usuarios)} user ids")

    except psycopg2.Error as e:
        print(f"ERROR: Couldnt fetch users from postgres.\n{e}")
        return

    print("Now will start transaction producer...")
    
    create_topic(TOPIC_NAME)
    producer = Producer(producer_config)
    
    transaction_count = 0
    print("Starting to generate and publish transactions in real-time...")
    
    try:
        while True:
            transaction = generate_transaction(usuarios)
            
            producer.produce(
                topic=TOPIC_NAME,
                value=json.dumps(transaction).encode('utf-8'),
                callback=delivery_report
            )
            
            producer.flush()
            
            transaction_count += 1
            if transaction_count % 100 == 0:
                print(f"Generated and published {transaction_count} transactions")
            
            time.sleep(0.5)
            
    except KeyboardInterrupt:
        print("\nStopping transaction producer...")
    except Exception as e:
        print(f"Error producing message: {e}")
    finally:
        print(f"Total transactions published: {transaction_count}")

if __name__ == "__main__":
    time.sleep(15)
    main()
