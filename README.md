# Banking Transaction Stream

Sistema de streaming de transações bancárias em tempo real usando Apache Kafka, simulando transações PIX, TED, DOC e Boleto no Brasil.

## Como Usar

1. **Iniciar serviços:**
   ```bash
   # Garantir que nenhum serviço antigo está rodando
   docker compose down -v
   
   # Iniciar todos os serviços
   docker compose up --build  

   ou

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
  "id_transacao": "UUID único para cada transação",
  "id_usuario_pagador": "UUID do usuário que está pagando",
  "id_usuario_recebedor": "UUID do usuário que está recebendo",
  "id_regiao": "UF do estado brasileiro (ex: SP, RJ, MG, etc.)",
  "modalidade_pagamento": "PIX, TED, DOC ou Boleto",
  "data_horario": "Timestamp em milissegundos",
  "valor_transacao": "Valor da transação em reais (double)",
  "saldo_pagador": "Saldo do pagador em reais (double)",
  "limite_modalidade": "Limite disponível para a modalidade de pagamento (double)"
}
```

## Características do Sistema

- **Geração de Dados Consistente**: Usa seed fixo (42) para reprodutibilidade
- **Distribuição Geográfica**: Transações distribuídas pelos estados brasileiros
- **Limites por Modalidade**: Cada tipo de transação tem seu próprio limite
- **Validação de Saldo**: Verifica saldo disponível antes de cada transação
- **Distribuição de Valores**: Usa distribuição exponencial para valores realistas

## Serviços e Portas

- **Kafka Broker**: localhost:9092
- **Schema Registry**: localhost:8081 (Avro schema)
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
