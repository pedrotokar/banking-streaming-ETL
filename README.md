# Banking Transaction Streaming ETL

Este projeto implementa um sistema de streaming ETL para processamento de transações bancárias em tempo real usando Apache Kafka e Apache Spark.

## Arquitetura

O sistema é composto por vários componentes:

1. **Producer**: Gera transações bancárias simuladas e envia para o Kafka usando formato Avro
2. **Consumer**: Consome as mensagens, processa em batches e calcula scores de risco
3. **Apache Kafka**: Sistema de mensageria para streaming de dados
4. **Schema Registry**: Gerencia os schemas Avro
5. **Kafka UI**: Interface web para monitoramento do Kafka

## Portas e Serviços

- **Kafka UI**: http://localhost:8080
  - Interface web para monitoramento do Kafka
  - Visualize tópicos, consumidores, mensagens, etc.

- **Schema Registry**: http://localhost:8081
  - API REST para gerenciamento de schemas
  - Endpoint principal: `/subjects` para listar todos os schemas

- **Kafka Broker**: localhost:9092
  - Broker principal do Kafka
  - Usado para conexões dos produtores e consumidores

## Estrutura do Projeto

```
banking-streaming-ETL/
├── containers/
│   ├── consumer/         # Consumidor Kafka e processamento Spark
│   ├── producer/         # Gerador de transações
│   └── spark/           # Configurações do Spark
├── data/                # Schemas e dados
└── docker-compose.yml   # Configuração dos containers
```

## Como Executar

1. **Pré-requisitos**:
   - Docker
   - Docker Compose

2. **Iniciar o Sistema**:
   ```bash
   docker compose up --build
   ```

3. **Parar o Sistema**:
   ```bash
   docker compose down -v
   ```

## Fluxo de Dados

1. O Producer gera transações bancárias aleatórias com os seguintes campos:
   - ID da transação
   - ID do pagador
   - ID do recebedor
   - Região
   - Modalidade de pagamento (PIX, TED, DOC, Boleto)
   - Data/hora
   - Valor
   - Saldo do pagador
   - Limite da modalidade
   - Dados geográficos
   - Métricas de risco

2. O Consumer:
   - Recebe as transações em formato Avro
   - Agrupa em batches de 1000 mensagens
   - Processa usando Spark para calcular scores de risco:
     - T5: Distância geográfica entre pagador e região cadastrada
     - T6: Score baseado em padrões regionais
     - T7: Score baseado no horário da transação

## Monitoramento

1. **Kafka UI** (http://localhost:8080):
   - Monitore o tópico `bank_transactions`
   - Verifique o throughput de mensagens
   - Acompanhe os consumidores

2. **Logs**:
   ```bash
   # Logs do producer
   docker compose logs -f producer

   # Logs do consumer
   docker compose logs -f consumer
   ```

## Dados Gerados

O sistema gera três arquivos CSV no diretório `/app/data` do container consumer:

1. `transacoes_100k.csv`: Transações processadas
2. `informacoes_cadastro_100k.csv`: Dados dos usuários
3. `regioes_estados_brasil.csv`: Informações das regiões

## Troubleshooting

1. **Erro de Conexão com Kafka**:
   - Verifique se todos os serviços estão rodando: `docker compose ps`
   - Aguarde 30 segundos após o início para o Kafka estar pronto

2. **Erros de Schema**:
   - Verifique o Schema Registry: http://localhost:8081/subjects
   - Confirme se o schema está registrado corretamente

3. **Sem Processamento**:
   - Verifique os logs do consumer
   - Confirme se as mensagens estão chegando no Kafka UI
