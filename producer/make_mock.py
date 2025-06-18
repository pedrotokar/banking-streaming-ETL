import pandas as pd
import numpy as np
import os
import uuid
from datetime import datetime, timedelta
import sqlite3


output_dir = "data/"
os.makedirs(output_dir, exist_ok=True)
np.random.seed(42)  

# Tamanhos de geração
sizes = {
    #"1k": 1_000,
    "100k": 100_000,
    #"1M": 1_000_000,
    #"3M": 3_000_000
}

# Tipos de transação conforme schema Avro
TRANSACTION_TYPES = ["DEPOSIT", "WITHDRAWAL", "TRANSFER"]

payment_methods = ["PIX", "TED", "DOC", "Boleto"]
estados_uf = [
    "AC","AL","AP","AM","BA","CE","DF","ES","GO","MA","MT",
    "MS","MG","PA","PB","PR","PE","PI","RJ","RN","RS","RO",
    "RR","SC","SP","SE","TO"
]


# se False, gera limites independentes ou dependentes por modalidade
same_limit_for_all = True


# 1) Tabela 3: Regiões
region_list = []
for uf in estados_uf:
    lat = np.round(-34 + np.random.rand() * 39, 6)
    lon = np.round(-74 + np.random.rand() * 40, 6)
    media_mens = np.round(1_000 + np.random.rand() * 30_000, 2)
    num_fraude = int(np.random.randint(0, 100))
    region_list.append([uf, lat, lon, media_mens, num_fraude])

df_regions = pd.DataFrame(
    region_list,
    columns=[
        "id_regiao",
        "latitude",
        "longitude",
        "media_transacional_mensal",
        "num_fraudes_ult_30d"
    ]
)
regions_csv = os.path.join(output_dir, "regioes_estados_brasil.csv")
df_regions.to_csv(regions_csv, index=False)
print(f"Tabela 3 salva em: {regions_csv}")


for label, N in sizes.items():
    print(f"\n=== Gerando mocks para {label} ({N} transações) ===")

    # Gerar transações
    transactions = {
        "transaction_id": [str(uuid.uuid4()) for _ in range(N)],
        "timestamp": [int((datetime.now() - timedelta(days=np.random.randint(0, 30))).timestamp() * 1000) for _ in range(N)],
        "account_id": [str(uuid.uuid4()) for _ in range(N)],
        "amount": np.round(np.random.exponential(scale=1000, size=N), 2),
        "transaction_type": np.random.choice(TRANSACTION_TYPES, size=N),
        "current_balance": np.round(np.random.uniform(1000, 100000, size=N), 2)
    }

    df_transactions = pd.DataFrame(transactions)

    # Salvar como CSV
    tx_csv = os.path.join(output_dir, f"transacoes_{label}.csv")
    df_transactions.to_csv(tx_csv, index=False)
    print(f"Transações salvas em: {tx_csv}")

    # Salvar como SQLite
    sql_path = os.path.join(output_dir, f"transacoes_{label}.db")
    con = sqlite3.connect(sql_path)
    cursor = con.cursor()

    # Criar tabela
    cursor.execute("DROP TABLE IF EXISTS transactions")
    cursor.execute("""
        CREATE TABLE transactions (
            transaction_id TEXT PRIMARY KEY,
            timestamp INTEGER,
            account_id TEXT,
            amount REAL,
            transaction_type TEXT,
            current_balance REAL
        )
    """)

    # Inserir dados
    for _, row in df_transactions.iterrows():
        cursor.execute(
            "INSERT INTO transactions (transaction_id, timestamp, account_id, amount, transaction_type, current_balance) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (
                row.transaction_id,
                row.timestamp,
                row.account_id,
                row.amount,
                row.transaction_type,
                row.current_balance
            )
        )

    con.commit()
    con.close()
    print(f"Transações salvas em SQLite: {sql_path}")

print("\nGeração de dados mock concluída.")
