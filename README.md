# Banking Streaming ETL

-------------------------------------------------------------------------------

## Sobre o repositório

Esse repositório é destinado à segunda avaliação da matéria de Computação
Escalável, lecionada no 5º período da graduação de Ciência de Dados e
Inteligência Artificial da FGV-EMAp.

O trabalho consiste em reaproveitar a pipeline modelada pelo grupo no primeiro
trabalho e implementá-la usando ferramentas de mercado de computação distribuída
e fazer deploy na nuvem.

As ferramentas utilizadas por nosso grupo até o momento foram:

- Kafka para gerenciamento do broker;
- Confluent para auxiliar na comunicação dos produtores de dados e o broker;
- Spark Structured Streaming para execução das operações da pipeline;
- PostgreSQL para armazenamento de dados históricos;
- Docker e Docker Compose para gerenciamento dos componentes;
- Streamlit para geração do dashboard.

## Como executar

Como fizemos uso do Docker Compose e de imagens de contêineres disponíveis
publicamente, a execução de toda a pipeline consiste em utilizar um comando para
iniciar os contêineres Docker. Para garantir que não há nenhum container sendo
executado atualmente, utilize:

```bash
$ docker compose down -v
```

Caso seja a primeira vez executando, é necessário dar build nas imagens. Para
isso:

```bash
$ docker compose up --build
```

Cao não seja a primeira vez executando e não tenham havido mudanças desde a última
execução, basta utilizar

```bash
$ docker compose up
```

*OBS:* o banco de dados depende de uma tabela de usuários que é construída
pelo script `historic_data_mock.py`. Então, durante a primeira execução do docker,
é necessário rodar o script, verificar se a saída é positiva e então reiniciar
os contêineres. Após isso, o banco de dados estará populado e os sistemas
funcionarão como desejado.

Para verificar que os produtores estão operando corretamente e que o broker está
recebendo transações, é possível:

- Abrir Kafka UI: http://localhost:8080
- Clicar em "Topics"
- Selecionar tópico: `bank_transactions`
- Ver se há mensagens chegando em tempo real.

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
  "valor_transacao": "Valor da transação em reais (double)"
}
```
E são escritas no banco de dados PostgreSQL com uma coluna extra indicando se
ela foi aprovada ou não.

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
- **PostgreSQL**: localhost:5432
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
