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

np.random.seed(42)

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

            # cur.execute("""
            #     DROP TABLE IF EXISTS transacoes CASCADE;
            # """)

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
                    limite_Boleto NUMERIC(15, 2) NOT NULL DEFAULT 0.00,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
                    transacao_aprovada BOOL NOT NULL,
                    -- Novas colunas para métricas de tempo
                    tempo_saida_resultado TIMESTAMP,
                    tempo_entrada_kafka TIMESTAMP,
                    tempo_inicio_processamento TIMESTAMP,
                    latencia_total_ms BIGINT,
                    tempo_processamento_ms BIGINT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)

            # Criar índices para otimizar consultas de performance
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_transacoes_data_horario 
                ON transacoes(data_horario);
            """)

            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_transacoes_latencia 
                ON transacoes(latencia_total_ms);
            """)

            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_transacoes_aprovada 
                ON transacoes(transacao_aprovada);
            """)

            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_transacoes_modalidade 
                ON transacoes(modalidade_pagamento);
            """)

            print("Tables created with timing metrics columns.")

            # Criar uma view para estatísticas de performance
            cur.execute("""
                CREATE OR REPLACE VIEW vw_performance_stats AS
                SELECT 
                    modalidade_pagamento,
                    COUNT(*) as total_transacoes,
                    AVG(latencia_total_ms) as latencia_media_ms,
                    MIN(latencia_total_ms) as latencia_minima_ms,
                    MAX(latencia_total_ms) as latencia_maxima_ms,
                    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY latencia_total_ms) as latencia_p95_ms,
                    AVG(tempo_processamento_ms) as tempo_processamento_medio_ms,
                    COUNT(CASE WHEN transacao_aprovada THEN 1 END) as transacoes_aprovadas,
                    COUNT(CASE WHEN NOT transacao_aprovada THEN 1 END) as transacoes_rejeitadas,
                    ROUND(
                        (COUNT(CASE WHEN transacao_aprovada THEN 1 END) * 100.0 / COUNT(*)), 2
                    ) as taxa_aprovacao_pct
                FROM transacoes 
                WHERE latencia_total_ms IS NOT NULL
                GROUP BY modalidade_pagamento;
            """)

            # Criar view para análise temporal
            cur.execute("""
                CREATE OR REPLACE VIEW vw_performance_temporal AS
                SELECT 
                    DATE_TRUNC('hour', tempo_saida_resultado) as hora,
                    COUNT(*) as transacoes_por_hora,
                    AVG(latencia_total_ms) as latencia_media_ms,
                    MAX(latencia_total_ms) as latencia_maxima_ms,
                    AVG(tempo_processamento_ms) as tempo_processamento_medio_ms
                FROM transacoes 
                WHERE tempo_saida_resultado IS NOT NULL 
                  AND latencia_total_ms IS NOT NULL
                GROUP BY DATE_TRUNC('hour', tempo_saida_resultado)
                ORDER BY hora DESC;
            """)

            print("Performance views created.")

            psycopg2.extras.execute_values(
                cur,
                """INSERT INTO usuarios (id_usuario, id_regiao, saldo, limite_PIX, limite_TED, limite_DOC, limite_Boleto)
                VALUES %s
                ON CONFLICT (id_usuario) DO NOTHING;""",
                insert_tuples,
                template = None,
                page_size = 100
            )

            print("User data inserted successfully.")

            # Mostrar estatísticas das tabelas criadas
            cur.execute("SELECT COUNT(*) FROM usuarios;")
            user_count = cur.fetchone()['count']
            print(f"Total de usuários criados: {user_count}")

            print("Database setup completed with timing metrics support!")

except psycopg2.Error as e:
    print(f"Problem while doing postgres operations: {e}")
else:
    print("-" * 50)
    print("Database successfully created with timing metrics!")
    print("Performance views available:")
    print("   - vw_performance_stats: Estatísticas por modalidade")
    print("   - vw_performance_temporal: Análise temporal por hora")
    print("Função de limpeza disponível:")
    print("   - limpar_transacoes_antigas(dias): Remove dados antigos")
    print("-" * 50)