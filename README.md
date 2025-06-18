# Banking Transaction Stream

Sistema de streaming de transações bancárias em tempo real usando Apache Kafka.

## Como Usar

1. **Iniciar serviços:**
   ```bash
   # Garantir que nenhum serviço antigo está rodando
   docker compose down -v
   
   # Iniciar todos os serviços
   docker compose up
   ```

2. **Verificar o streaming:**
   - Abrir Kafka UI: http://localhost:8080
   - Clicar em "Topics"
   - Selecionar tópico: `bank_transactions`
   - Ver mensagens chegando em tempo real

## Estrutura dos Dados

Cada transação contém:
```json
{
  "transaction_id": "uuid string",
  "timestamp": "unix timestamp em ms",
  "account_id": "uuid string",
  "amount": "valor decimal",
  "transaction_type": "DEPOSIT/WITHDRAWAL/TRANSFER",
  "current_balance": "valor decimal"
}
```

## Serviços e Portas

- **Kafka Broker**: localhost:9092
- **Schema Registry**: localhost:8081
- **Kafka UI**: localhost:8080
- **Producer**: Serviço interno que gera transações em tempo real

## Troubleshooting

1. **Problemas com Docker:**
   ```bash
   # Parar e remover tudo
   docker compose down -v
   docker system prune -f
   
   # Reiniciar serviços
   docker compose up
   ```

2. **Verificar logs:**
   ```bash
   # Ver logs específicos do producer
   docker compose logs -f producer
   
   # Ver logs do Kafka
   docker compose logs -f broker
   ```

3. **Problemas comuns:**
   - Se o Kafka UI não abrir, aguarde 30 segundos e recarregue
   - Se o producer não conectar, verifique os logs e reinicie
   - Se precisar parar, use Ctrl+C e depois `docker compose down -v`

## Limpeza

Para parar todos os serviços e limpar dados:
```bash
docker compose down -v
```
