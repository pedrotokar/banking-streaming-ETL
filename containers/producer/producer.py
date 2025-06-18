# Arquivo de teste, precisa mudar depois

import os
import json
import time
import pandas as pd
from datetime import datetime
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
from confluent_kafka.admin import AdminClient, NewTopic

schema_path = "/app/data/schema.avsc"
with open(schema_path) as f:
    schema_str = f.read()

KAFKA_BROKER = 'broker:29092'
SCHEMA_REGISTRY_URL = 'http://schema-registry:8081'
TOPIC_NAME = 'bank_transactions'

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
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}] @ {msg.offset()}')

def read_transactions(csv_path):
    """Read transactions from CSV file."""
    return pd.read_csv(csv_path)

def main():
    print("Starting transaction producer...")
    
    create_topic(TOPIC_NAME)
    
    avro_producer = AvroProducer(
        producer_config,
        default_value_schema=avro.loads(schema_str)
    )
    
    csv_path = "/app/data/transacoes_100k.csv"
    df_transactions = read_transactions(csv_path)
    total_transactions = len(df_transactions)
    
    print(f"Loaded {total_transactions} transactions from CSV")
    
    transaction_count = 0
    while True:
        try:
            row = df_transactions.iloc[transaction_count % total_transactions]
            
            transaction = {
                'transaction_id': row['transaction_id'],
                'timestamp': int(row['timestamp']),
                'account_id': row['account_id'],
                'amount': float(row['amount']),
                'transaction_type': row['transaction_type'],
                'current_balance': float(row['current_balance'])
            }
            
            avro_producer.produce(
                topic=TOPIC_NAME,
                value=transaction,
                callback=delivery_report
            )
            
            avro_producer.flush()
            
            transaction_count += 1
            if transaction_count % 100 == 0:
                print(f"Published {transaction_count} transactions")
            
            time.sleep(0.5)
            
        except Exception as e:
            print(f"Error producing message: {e}")
            time.sleep(3)  

if __name__ == "__main__":
    time.sleep(15)
    main()

