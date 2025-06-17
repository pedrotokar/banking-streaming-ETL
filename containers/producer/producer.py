# Arquivo de teste, precisa mudar depois

import time
from faker import Faker
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

# Kafka
BOOTSTRAP_SERVERS = 'broker:29092'
SCHEMA_REGISTRY_URL = 'http://schema-registry:8081'
TRANSACTION_TOPIC = 'bank_transactions_raw'

# Esquema Avro
SCHEMA_PATH = 'data/schema.avsc'
with open(SCHEMA_PATH, 'r') as f:
    VALUE_SCHEMA_STR = f.read()

def delivery_report(err, msg):
    """Callback para relatar a entrega da mensagem."""
    if err is not None:
        print(f"Mensagem falhou na entrega: {err}")
    else:
        print(f"Mensagem entregue para {msg.topic()} [{msg.partition()}] @ {msg.offset()}")

def main():
    fake = Faker('pt_BR')

    schema_registry_conf = {'url': SCHEMA_REGISTRY_URL}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    avro_serializer = AvroSerializer(schema_registry_client, VALUE_SCHEMA_STR)

    producer_conf = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'value.serializer': avro_serializer
    }
    producer = SerializingProducer(producer_conf)

    print(f"Produzindo transações para o tópico: {TRANSACTION_TOPIC}")

    account_balances = {}
    for _ in range(5):
        account_balances[fake.uuid4()] = fake.random_int(min=500, max=50000) / 100.0

    try:
        while True:
            account_id = fake.random_element(elements=list(account_balances.keys()))
            if fake.boolean(chance_of_getting_true=10):
                new_account_id = str(fake.uuid4())
                account_balances[new_account_id] = fake.random_int(min=100, max=10000) / 100.0
                account_id = new_account_id

            transaction_id = str(fake.uuid4())
            timestamp = int(time.time() * 1000)
            
            transaction_type = fake.random_element(elements=('DEPOSIT', 'WITHDRAWAL', 'TRANSFER'))
            
            amount = 0.0
            if transaction_type == 'DEPOSIT':
                amount = round(fake.random_int(min=10, max=1000) / 100.0, 2)
                account_balances[account_id] += amount
            elif transaction_type == 'WITHDRAWAL':
                amount = round(fake.random_int(min=5, max=500) / 100.0, 2)
            elif transaction_type == 'TRANSFER':
                amount = round(fake.random_int(min=10, max=750) / 100.0, 2)

            current_balance = round(account_balances[account_id], 2)

            transaction_data = {
                "transaction_id": transaction_id,
                "timestamp": timestamp,
                "account_id": account_id,
                "amount": amount,
                "transaction_type": transaction_type,
                "current_balance": current_balance
            }

            producer.produce(
                topic=TRANSACTION_TOPIC,
                value=transaction_data,
                key=account_id,
                on_delivery=delivery_report
            )

            producer.poll(0)

            print(f"Transação enviada: ID={transaction_data['transaction_id']}, Conta={transaction_data['account_id']}, Tipo={transaction_data['transaction_type']}, Valor={transaction_data['amount']:.2f}, Saldo Antes={transaction_data['current_balance']:.2f}")
            time.sleep(0.5)

    except KeyboardInterrupt:
        print("Produção interrompida")
    finally:
        producer.flush()
        print("Produtor finalizado")

if __name__ == "__main__":
    main()

