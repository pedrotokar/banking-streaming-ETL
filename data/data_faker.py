# data_faker.py
import pandas as pd
import numpy as np
from faker import Faker
from random import choice, random
import os

# Initialize
fake = Faker('pt_BR')
np.random.seed(42)

# Settings
N = 10_000
TRANSACTION_TYPES = ['PIX', 'TED', 'DOC', 'Debit Card', 'Credit Card']
USERS = [fake.uuid4() for _ in range(2000)]

# Rough lat/lon boundaries for Brazil
LAT_RANGE = (-33.75, -2.5)
LON_RANGE = (-73.9, -34.7)

def random_coordinates():
    lat = round(np.random.uniform(*LAT_RANGE), 6)
    lon = round(np.random.uniform(*LON_RANGE), 6)
    return lat, lon

def generate_row():
    user_id = choice(USERS)
    trans_type = choice(TRANSACTION_TYPES)
    amount = round(np.random.exponential(scale=300), 2)
    datetime_tx = fake.date_time_between(start_date='-30d', end_date='now')

    # Geographic points
    lat_sender, lon_sender = random_coordinates()
    lat_receiver = lat_sender + np.random.normal(0, 0.5)
    lon_receiver = lon_sender + np.random.normal(0, 0.5)

    # Risk scores (simplified)
    t7_score = (datetime_tx.hour - 12) / 12
    value_risk = min(1.0, amount / 5000)
    dist_risk = np.sqrt((lat_sender - lat_receiver)**2 + (lon_sender - lon_receiver)**2)

    # Fake approval logic
    approval_chance = 0.8 - 0.3 * value_risk - 0.2 * abs(t7_score) - 0.1 * min(dist_risk, 5)
    approved = random() < approval_chance

    return {
        "id_transaction": fake.uuid4(),
        "payer_id": user_id,
        "transaction_type": trans_type,
        "transaction_amount": amount,
        "timestamp": datetime_tx,
        "latitude_payer": lat_sender,
        "longitude_payer": lon_sender,
        "latitude_receiver": lat_receiver,
        "longitude_receiver": lon_receiver,
        "approved": approved
    }

# Generate dataset
print("Generating fake transactions...")
data = [generate_row() for _ in range(N)]
df = pd.DataFrame(data)

# Save to CSV
path = 'data/fake_transactions.csv'
os.makedirs("data", exist_ok=True)
df.to_csv(path, index=False)
print(f"Saved fake data to '{path}'")
