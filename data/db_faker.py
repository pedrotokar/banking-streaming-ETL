# generate_fake_db.py
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import random

# Parameters
n = 10000  # number of transactions
users = [f"user_{i}" for i in range(100)]
TRANSACTION_TYPES = ['PIX', 'TED', 'DOC', 'Debit Card', 'Credit Card']
start_time = datetime(2025, 1, 1)

# Generate fake data
data = []
for _ in range(n):
    payer = random.choice(users)
    receiver = random.choice(users)
    while receiver == payer:
        receiver = random.choice(users)
    timestamp = start_time + timedelta(minutes=random.randint(0, 60 * 24 * 30))
    amount = round(random.uniform(10, 50000), 2)
    approved = random.random() > 0.2  # 80% approval rate
    transaction_type = random.choice(TRANSACTION_TYPES)
    lat_payer = random.uniform(-33.7, 5.3)
    lon_payer = random.uniform(-73.9, -34.8)
    lat_receiver = random.uniform(-33.7, 5.3)
    lon_receiver = random.uniform(-73.9, -34.8)

    data.append([
        payer, receiver, timestamp, amount, approved, transaction_type,
        lat_payer, lon_payer, lat_receiver, lon_receiver
    ])

# Create DataFrame
df_fake = pd.DataFrame(data, columns=[
    "payer_id", "receiver_id", "timestamp", "amount", "approved", "transaction_type",
    "latitude_payer", "longitude_payer", "latitude_receiver", "longitude_receiver"
])

# Save to SQLite
db_path = "data/fake_bank.db"
conn = sqlite3.connect(db_path)
df_fake.to_sql("transactions", conn, if_exists="replace", index=False)
conn.close()

print(f"Fake database created at: {db_path}")
