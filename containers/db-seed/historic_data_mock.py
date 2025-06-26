import pandas as pd
import psycopg2
import psycopg2.extras
import uuid
import numpy as np
import os
import sys
import time
import json
from datetime import datetime

# --- STATIC ---
NUM_USERS_MOCK = 10_000
estados_uf = [
    "AC","AL","AP","AM","BA","CE","DF","ES","GO","MA","MT",
    "MS","MG","PA","PB","PR","PE","PI","RJ","RN","RS","RO",
    "RR","SC","SP","SE","TO"
]

np.random.seed(42)

# Configuração do PostgreSQL usando variáveis de ambiente
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'postgres'),
    'port': os.getenv('DB_PORT', '5432'),
    'dbname': os.getenv('DB_NAME', 'bank'),
    'user': os.getenv('DB_USER', 'bank_etl'),
    'password': os.getenv('DB_PASS', 'ihateavroformat123')
}

print(f"Connecting to PostgreSQL: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}")

def create_database_schema(cursor):
    """Cria o schema do banco de dados (tabelas e indices) de forma idempotente."""
    print("Garantindo que o schema do banco de dados (tabelas e indices) exista...")
    
    cursor.execute("""
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

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS transacoes (
            id_transacao UUID PRIMARY KEY,
            id_usuario_pagador UUID REFERENCES usuarios(id_usuario),
            id_usuario_recebedor UUID REFERENCES usuarios(id_usuario),
            id_regiao VARCHAR(2) NOT NULL,
            modalidade_pagamento VARCHAR(6) NOT NULL,
            data_horario TIMESTAMP NOT NULL,
            valor_transacao NUMERIC(15, 2) NOT NULL DEFAULT 0.00,
            transacao_aprovada BOOL NOT NULL,
            tempo_saida_resultado TIMESTAMP,
            tempo_entrada_kafka TIMESTAMP,
            tempo_inicio_processamento TIMESTAMP,
            latencia_total_ms BIGINT,
            tempo_processamento_ms BIGINT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS transacoes_scores (
            id_transacao UUID REFERENCES transacoes(id_transacao),
            t5_score FLOAT,
            t6_score FLOAT,
            t7_score FLOAT
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS regioes (
            id_regiao VARCHAR(2) PRIMARY KEY,
            latitude FLOAT NOT NULL,
            longitude FLOAT NOT NULL,
            media_transacional_mensal NUMERIC(15, 2) NOT NULL,
            num_fraudes_ult_30d INT NOT NULL
            );
    """)

    # Criar indices
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_transacoes_data_horario ON transacoes(data_horario);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_transacoes_latencia ON transacoes(latencia_total_ms);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_transacoes_aprovada ON transacoes(transacao_aprovada);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_transacoes_modalidade ON transacoes(modalidade_pagamento);")


    print("Schema verificado com sucesso.")

def create_performance_views(cursor):
    """Cria ou substitui as views e funcoes de performance."""
    print("Garantindo a existência de views e funcoes de performance...")
    # View de estatisticas por modalidade
    cursor.execute("""
        CREATE OR REPLACE VIEW vw_performance_stats AS
        SELECT 
            modalidade_pagamento,
            COUNT(*) as total_transacoes,
            AVG(latencia_total_ms) as latencia_media_ms,
            MIN(latencia_total_ms) as latencia_minima_ms,
            MAX(latencia_total_ms) as latencia_maxima_ms,
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY latencia_total_ms) as latencia_p95_ms,
            AVG(tempo_processamento_ms) as tempo_processamento_medio_ms
        FROM transacoes 
        WHERE latencia_total_ms IS NOT NULL
        GROUP BY modalidade_pagamento;
    """)
    # View para analise temporal
    cursor.execute("""
        CREATE OR REPLACE VIEW vw_performance_temporal AS
        SELECT 
            DATE_TRUNC('hour', tempo_saida_resultado) as hora,
            COUNT(*) as transacoes_por_hora,
            AVG(latencia_total_ms) as latencia_media_ms
        FROM transacoes 
        WHERE tempo_saida_resultado IS NOT NULL AND latencia_total_ms IS NOT NULL
        GROUP BY DATE_TRUNC('hour', tempo_saida_resultado)
        ORDER BY hora DESC;
    """)
    
    cursor.execute("""
        CREATE OR REPLACE FUNCTION limpar_transacoes_antigas(dias_retencao INTEGER DEFAULT 30)
        RETURNS INTEGER AS $$
        DECLARE
            registros_removidos INTEGER;
        BEGIN
            DELETE FROM transacoes 
            WHERE created_at < NOW() - (dias_retencao * INTERVAL '1 day');
            
            GET DIAGNOSTICS registros_removidos = ROW_COUNT;
            
            RETURN registros_removidos;
        END;
        $$ LANGUAGE plpgsql;
    """)
    print("Views e funcoes de performance verificadas.")

def generate_user_data(same_limit_for_all=True):
    """Gera dados de usuario aleatorios."""
    user_id = str(uuid.uuid4())
    saldo = round(np.random.exponential(scale=5000), 2)
    base_limit = round(100 + np.random.exponential(scale=5000), 2)
    limites = {"limite_PIX": base_limit, "limite_TED": base_limit, "limite_DOC": base_limit, "limite_Boleto": base_limit}
    regiao = np.random.choice(estados_uf)
    return {"id_usuario": user_id, "id_regiao": regiao, "saldo": saldo, **limites}

def insert_user_data(cursor):
    """Insere dados de usuarios mock."""
    print(f"Gerando e inserindo {NUM_USERS_MOCK:,} usuarios...")
    generated_users = [generate_user_data() for _ in range(NUM_USERS_MOCK)]
    columns = ["id_usuario", "id_regiao", "saldo", "limite_PIX", "limite_TED", "limite_DOC", "limite_Boleto"]
    insert_tuples = [tuple(user[col] for col in columns) for user in generated_users]
    psycopg2.extras.execute_values(
        cursor,
        "INSERT INTO usuarios (id_usuario, id_regiao, saldo, limite_PIX, limite_TED, limite_DOC, limite_Boleto) VALUES %s",
        insert_tuples,
        page_size=1000
    )
    print("Dados de usuarios inseridos com sucesso.")

def insert_region_data(cursor):
    print("Inserindo  informações de região.")
    DADOS_REGIOES = [
        ("AC", 4.147298,-35.642456,26305.75,82),
        ("AL", -29.24522,-67.13095,4958.37,22),
        ("AP", -28.700766,-34.714225,4037.06,89),
        ("AM", 3.598371,-64.437367,10351.37,27),
        ("BA", -20.841066,-62.998829,6068.45,0),
        ("CE", -17.510177,-55.930461,23148.82,97),
        ("DF", -28.12076,-58.332415,25596.55,80),
        ("ES", -32.577558,-66.29333,13999.48,31),
        ("GO", -10.233171,-50.206164,20182.09,67),
        ("MA", -10.735464,-41.078448,22756.66,7),
        ("MT", -1.077585,-55.420304,6304.58,89),
        ("MS", -3.859656,-37.757044,20954.34,93),
        ("MG", -17.570166,-51.046926,10117.24,13),
        ("PA", -3.988757,-67.33787,21335.66,19),
        ("PB", 0.97572,-72.101982,17141.8,33),
        ("PR", -1.547698,-56.028711,5568.68,80),
        ("PE", -1.811759,-47.714975,17996.38,40),
        ("PI", -14.570089,-65.483063,9340.64,89),
        ("RJ", -9.712581,-52.622654,18112.04,63),
        ("RN", 2.104537,-72.836875,24662.22,57),
        ("RS", -23.698082,-65.421225,8541.55,38),
        ("RO", -12.737473,-35.20106,21561.12,98),
        ("RR", -1.50035,-35.527608,27278.9,26),
        ("SC", 2.597277,-45.369319,13098.97,82),
        ("SP", -18.333825,-34.478437,23166.88,36),
        ("SE", -29.278844,-42.949481,4835.22,2),
        ("TO", -29.697094,-51.37746,18537.89,12),
        ]
    psycopg2.extras.execute_values(
        cursor,
        "INSERT INTO regioes (id_regiao, latitude, longitude, media_transacional_mensal, num_fraudes_ult_30d) VALUES %s ON CONFLICT (id_regiao) DO NOTHING;",
        DADOS_REGIOES,
        page_size=1000
        )
    print("Dados de regioes inseridos com sucesso.")

def main():
    """Funcao principal para configurar o banco de dados."""
    conn_params = {
        "host": os.getenv("DB_HOST", "postgres"),
        "port": os.getenv("DB_PORT", "5432"),
        "dbname": os.getenv("DB_NAME", "bank"),
        "user": os.getenv("DB_USER", "bank_etl"),
        "password": os.getenv("DB_PASS", "ihateavroformat123"),
        "connect_timeout": 10
    }
    force_recreate = os.getenv("FORCE_RECREATE", "false").lower() == "true"

    try:
        print("Conectando ao servidor PostgreSQL...")
        with psycopg2.connect(**conn_params) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                print("Conexao estabelecida com sucesso!")

                if force_recreate:
                    print("FORCE_RECREATE=true. Limpando tabelas existentes...")
                    cur.execute("DROP TABLE IF EXISTS transacoes CASCADE;")
                    cur.execute("DROP TABLE IF EXISTS transacoes_scores CASCADE;")
                    cur.execute("DROP TABLE IF EXISTS usuarios CASCADE;")
                    print("Tabelas antigas removidas.")

                create_database_schema(cur)
                create_performance_views(cur)

                # Verificar se o banco de dados precisa ser populado
                cur.execute("SELECT COUNT(*) as count FROM usuarios;")
                user_count = cur.fetchone()['count']

                if user_count == 0:
                    print("Banco de dados vazio detectado. Populando com dados iniciais...")
                    insert_user_data(cur)
                    insert_region_data(cur)
                else:
                    print(f"O banco de dados ja esta populado com {user_count:,} usuarios. Nenhuma acao necessaria.")
                    print("Use a variavel de ambiente FORCE_RECREATE=true para forcar a recriacao.")
                    # time.sleep(15)
                
                conn.commit()

    except psycopg2.OperationalError as e:
        print(f"Erro de conexao com o PostgreSQL: {e}. O container esta de pe? Tentando novamente em 5s...", file=sys.stderr)
        time.sleep(5)
        sys.exit(1)
    except psycopg2.Error as e:
        print(f"Erro no PostgreSQL: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Erro inesperado: {e}", file=sys.stderr)
        sys.exit(1)
    
    print("-" * 60)
    print("Setup do banco de dados concluido com sucesso!")
    print("Use FORCE_RECREATE=true para recriar o banco do zero.")
    print("-" * 60)

if __name__ == "__main__":
    main()
