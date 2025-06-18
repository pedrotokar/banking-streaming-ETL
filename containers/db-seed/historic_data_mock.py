#TODO: Fazer tabela de transações histórica e calcular saldos baseado nelas para consistência

import psycopg2
import psycopg2.extras

import uuid
import numpy as np

NUM_USERS_MOCK = 100_000
estados_uf = [
    "AC","AL","AP","AM","BA","CE","DF","ES","GO","MA","MT",
    "MS","MG","PA","PB","PR","PE","PI","RJ","RN","RS","RO",
    "RR","SC","SP","SE","TO"
]

def generate_user_data(same_limit_for_all = True):
    """Generate random user data."""
    user_id = str(uuid.uuid4())
    saldo = round(np.random.exponential(scale=5000), 2)

    if same_limit_for_all:
        base_limit = round(100 + np.random.exponential(scale=5000), 2)
        limites = {
            "limite_PIX": base_limit,
            "limite_TED": base_limit,
            "limite_DOC": base_limit,
            "limite_Boleto": base_limit
            }
    else:
        limites = {
            "limite_PIX": round(100 + np.random.exponential(scale=5000), 2),
            "limite_TED": round(100 + np.random.exponential(scale=5000), 2),
            "limite_DOC": round(100 + np.random.exponential(scale=5000), 2),
            "limite_Boleto": round(100 + np.random.exponential(scale=5000), 2)
            }

    regiao = np.random.choice(estados_uf)

    return {
        "id_usuario": user_id,
        "id_regiao": regiao,
        "saldo": saldo,
        **limites,
    }

generated_users = [generate_user_data() for _ in range(NUM_USERS_MOCK)]
columns = ["id_usuario", "id_regiao", "saldo", "limite_PIX",
           "limite_TED", "limite_DOC", "limite_Boleto"]
insert_tuples = [
    tuple(user[col] for col in columns) for user in generated_users
]

#postgres connection info
conn_params = {
    "host": "postgres",
    "port": "5432",
    "dbname": "bank",
    "user": "bank_etl",
    "password": "ihateavroformat123"
}

try:
    print("Connecting to postgres sql server...")

    with psycopg2.connect(**conn_params) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:

            print("Set up completed, access to postgres sql server done")

            cur.execute("""
                DROP TABLE IF EXISTS usuarios CASCADE;
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS usuarios (
                    id_usuario UUID PRIMARY KEY,
                    id_regiao VARCHAR(2) NOT NULL,
                    saldo NUMERIC(15, 2) NOT NULL DEFAULT 0.00,
                    limite_PIX NUMERIC(15, 2) NOT NULL DEFAULT 0.00,
                    limite_TED NUMERIC(15, 2) NOT NULL DEFAULT 0.00,
                    limite_DOC NUMERIC(15, 2) NOT NULL DEFAULT 0.00,
                    limite_Boleto NUMERIC(15, 2) NOT NULL DEFAULT 0.00
                );
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS transacoes (
                    id_transacao UUID PRIMARY KEY,
                    id_usuario_pagador UUID REFERENCES usuarios(id_usuario),
                    id_usuario_recebedor UUID REFERENCES usuarios(id_usuario),
                    id_regiao VARCHAR(2) NOT NULL,
                    modalidade_pagamento VARCHAR(6) NOT NULL,
                    data_horario TIMESTAMP NOT NULL,
                    valor_transacao NUMERIC(15, 2) NOT NULL DEFAULT 0.00,
                    transacao_aprovada BOOL NOT NULL
                );
            """)

            print("Users table is correct.")

            psycopg2.extras.execute_values(
                cur,
                """INSERT INTO usuarios (id_usuario, id_regiao, saldo, limite_PIX, limite_TED, limite_DOC, limite_Boleto)
                VALUES %s
                ON CONFLICT (id_usuario) DO NOTHING;""",
                insert_tuples,
                template = None,
                page_size = 100
            )

            print("Data inserted")


except psycopg2.Error as e:
    print(f"Problem while doing postgres operations: {e}")
else:
    print("-" * 30)
    print("Users mock successfully created.")
