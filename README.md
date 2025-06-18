# Banking Transaction Stream

Sistema de streaming de transações bancárias usando Apache Kafka.

## Como Usar

1. **Gerar dados de teste:**
   ```bash
   # Criar ambiente virtual
   python3 -m venv venv
   source venv/bin/activate
   
   # Instalar dependências
   pip install -r requirements.txt
   
   # Gerar dados mock
   python producer/make_mock.py
   ```

2. **Iniciar os serviços:**
   ```bash
   docker compose up -d
   ```

3. **Verificar o streaming:**
   - Abrir Kafka UI: http://localhost:8080
   - Ver mensagens no tópico: `bank_transactions`

## Estrutura dos Dados

Cada transação contém:
- ID da transação
- Timestamp
- ID da conta
- Valor
- Tipo (DEPOSIT, WITHDRAWAL, TRANSFER)
- Saldo atual

## Serviços

- **Kafka Broker**: Porta 9092
- **Schema Registry**: Porta 8081
- **Kafka UI**: Porta 8080
- **Producer**: Lê dados do CSV e publica no Kafka

## Troubleshooting

Se encontrar problemas:
1. Verificar se Docker está rodando
2. Parar e reiniciar os serviços:
   ```bash
   docker compose down
   docker compose up -d
   ```
3. Verificar logs:
   ```bash
   docker compose logs -f producer
   ```
