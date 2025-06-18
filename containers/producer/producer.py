# Arquivo de teste, precisa mudar depois

import os
import json
import time
import uuid
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
from confluent_kafka.admin import AdminClient, NewTopic

KAFKA_BROKER = 'broker:29092'
SCHEMA_REGISTRY_URL = 'http://schema-registry:8081'
TOPIC_NAME = 'bank_transactions'
OUTPUT_DIR = '/app/data'

np.random.seed(42)
payment_methods = ["PIX", "TED", "DOC", "Boleto"]
estados_uf = [
    "AC","AL","AP","AM","BA","CE","DF","ES","GO","MA","MT",
    "MS","MG","PA","PB","PR","PE","PI","RJ","RN","RS","RO",
    "RR","SC","SP","SE","TO"
]

regioes = {}
for uf in estados_uf:
    lat = np.round(-34 + np.random.rand() * 39, 6)
    lon = np.round(-74 + np.random.rand() * 40, 6)
    media_mens = np.round(1_000 + np.random.rand() * 30_000, 2)
    num_fraude = int(np.random.randint(0, 100))
    regioes[uf] = {
        "latitude": lat,
        "longitude": lon,
        "media_transacional_mensal": media_mens,
        "num_fraudes_ult_30d": num_fraude
    }

schema_path = "/app/data/schema.avsc"
with open(schema_path) as f:
    schema_str = f.read()

producer_config = {
    'bootstrap.servers': KAFKA_BROKER,
    'schema.registry.url': SCHEMA_REGISTRY_URL,
    'client.id': 'bank-transaction-producer'
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
        print(f'Erro: {err}')

def generate_user_data():
    """Generate random user data."""
    user_id = str(uuid.uuid4())
    saldo = round(np.random.exponential(scale=5000), 2)
    base_limit = round(100 + np.random.exponential(scale=5000), 2)
    regiao = np.random.choice(estados_uf)
    
    return {
        "id_usuario": user_id,
        "id_regiao": regiao,
        "saldo": saldo,
        "limite_PIX": base_limit,
        "limite_TED": base_limit,
        "limite_DOC": base_limit,
        "limite_Boleto": base_limit,
        **regioes[regiao]
    }

def generate_transaction():
    """Generate a single random transaction."""
    pagador = generate_user_data()
    recebedor = generate_user_data()
    
    valor = round(np.random.exponential(scale=1000), 2)
    
    if valor > pagador["saldo"]:
        valor = round(pagador["saldo"] * np.random.random(), 2)
    
    modalidade = np.random.choice(payment_methods)
    
    limite_key = f"limite_{modalidade}"
    if valor > pagador[limite_key]:
        valor = round(pagador[limite_key] * np.random.random(), 2)
    
    data_horario = int((datetime.now() - timedelta(
        seconds=int(np.random.rand() * 86400 * 365)
    )).timestamp() * 1000)
    
    return {
        'id_transacao': str(uuid.uuid4()),
        'id_usuario_pagador': pagador["id_usuario"],
        'id_usuario_recebedor': recebedor["id_usuario"],
        'id_regiao': pagador["id_regiao"],
        'modalidade_pagamento': modalidade,
        'data_horario': data_horario,
        'valor_transacao': valor,
        'saldo_pagador': pagador["saldo"],
        'limite_modalidade': pagador[limite_key],
        'latitude': pagador["latitude"],
        'longitude': pagador["longitude"],
        'media_transacional_mensal': pagador["media_transacional_mensal"],
        'num_fraudes_ult_30d': pagador["num_fraudes_ult_30d"]
    }

def save_initial_data():
    """Save user and region data to CSV files."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Generate and save user data
    users_data = []
    for _ in range(1000):  # Generate 1000 initial users
        user = generate_user_data()
        users_data.append({
            'id_usuario': user['id_usuario'],
            'id_regiao': user['id_regiao'],
            'saldo': user['saldo'],
            'limite_PIX': user['limite_PIX'],
            'limite_TED': user['limite_TED'],
            'limite_DOC': user['limite_DOC'],
            'limite_Boleto': user['limite_Boleto']
        })
    
    # Prepare region data
    regions_data = []
    for uf, data in regioes.items():
        regions_data.append({
            'id_regiao': uf,
            **data
        })
    
    # Save to CSV
    users_df = pd.DataFrame(users_data)
    regions_df = pd.DataFrame(regions_data)
    
    users_df.to_csv(os.path.join(OUTPUT_DIR, "informacoes_cadastro_100k.csv"), index=False)
    regions_df.to_csv(os.path.join(OUTPUT_DIR, "regioes_estados_brasil.csv"), index=False)

def main():
    print("Starting transaction producer...")
    
    create_topic(TOPIC_NAME)
    save_initial_data()  # Save initial data before starting to produce
    
    avro_producer = AvroProducer(
        producer_config,
        default_value_schema=avro.loads(schema_str)
    )
    
    try:
        while True:
            transaction = generate_transaction()
            
            avro_producer.produce(
                topic=TOPIC_NAME,
                value=transaction,
                callback=delivery_report
            )
            
            avro_producer.flush()
            
            time.sleep(0.1)  # 100ms entre transações
            
    except KeyboardInterrupt:
        print("\nStopping transaction producer...")
    except Exception as e:
        print(f"Erro: {e}")

if __name__ == "__main__":
    time.sleep(15)
    main()