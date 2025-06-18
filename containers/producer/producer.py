# Arquivo de teste, precisa mudar depois

import os
import json
import time
import uuid
import random
from datetime import datetime, timedelta
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
from confluent_kafka.admin import AdminClient, NewTopic

# Load the Avro schema
schema_path = "/app/data/schema.avsc"
with open(schema_path) as f:
    schema_str = f.read()

# Kafka configuration
KAFKA_BROKER = 'broker:29092'
SCHEMA_REGISTRY_URL = 'http://schema-registry:8081'
TOPIC_NAME = 'bank_transactions'

# Configure the Avro producer
producer_config = {
    'bootstrap.servers': KAFKA_BROKER,
    'schema.registry.url': SCHEMA_REGISTRY_URL,
    'client.id': 'bank-transaction-producer'
}

def create_topic(topic_name, num_partitions=1, replication_factor=1):
    """Create Kafka topic if it doesn't exist."""
    admin_client = AdminClient({'bootstrap.servers': KAFKA_BROKER})
    
    # Check if topic exists
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

def generate_transaction():
    """Generate a single random transaction."""
    # Gerar timestamp dos últimos 30 dias
    timestamp = int((datetime.now() - timedelta(days=random.randint(0, 30))).timestamp() * 1000)
    
    # Gerar valores aleatórios
    transaction = {
        'transaction_id': str(uuid.uuid4()),
        'timestamp': timestamp,
        'account_id': str(uuid.uuid4()),
        'amount': round(random.uniform(10, 10000), 2),
        'transaction_type': random.choice(['DEPOSIT', 'WITHDRAWAL', 'TRANSFER']),
        'current_balance': round(random.uniform(1000, 100000), 2)
    }
    return transaction

def main():
    print("Starting transaction producer...")
    
    # Ensure topic exists
    create_topic(TOPIC_NAME)
    
    avro_producer = AvroProducer(
        producer_config,
        default_value_schema=avro.loads(schema_str)
    )
    
    transaction_count = 0
    print("Starting to generate and publish transactions in real-time...")
    
    try:
        while True:
            # Generate and send transaction
            transaction = generate_transaction()
            
            # Produce the message
            avro_producer.produce(
                topic=TOPIC_NAME,
                value=transaction,
                callback=delivery_report
            )
            
            avro_producer.flush()
            
            transaction_count += 1
            if transaction_count % 100 == 0:
                print(f"Generated and published {transaction_count} transactions")
            
            time.sleep(0.1) 
            
    except KeyboardInterrupt:
        print("\nStopping transaction producer...")
    except Exception as e:
        print(f"Error producing message: {e}")
    finally:
        print(f"Total transactions published: {transaction_count}")

if __name__ == "__main__":
    # Wait for broker to be ready
    time.sleep(15)
    main()

