# Banking Streaming ETL

*Desenvolvido por Anderson Falcão, Guilherme Castilho, Luan Carvalho, Pedro
Tokar, Tomás Lira e Vitor do Nascimento.*

-------------------------------------------------------------------------------

## Sobre o repositório

Esse repositório é destinado à segunda avaliação da matéria de Computação
Escalável, lecionada no 5º período da graduação de Ciência de Dados e
Inteligência Artificial da FGV-EMAp.

O trabalho consiste em reaproveitar a pipeline modelada pelo grupo no [primeiro
trabalho](https://github.com/TomasLira/Banking-Transactions-ETL) e implementá-la
usando ferramentas de mercado de computação distribuída, para então fazer deploy
na nuvem AWS.

As ferramentas de desenvolvimento e computação escalável utilizadas durante esse
trabalho foram:

- Kafka para gerenciamento do broker;
- Confluent para auxiliar na comunicação dos produtores de dados e o broker;
- Spark Structured Streaming para execução das operações da pipeline;
- PostgreSQL para armazenamento de dados históricos;
- Redis para armazenamento em memória dos dados processados pela pipeline;
- Docker e Docker Compose para gerenciamento dos componentes;
- Streamlit para criação do dashboard.

## Como executar localmente

*Observação: a depender das configurações do sistema, os comandos do docker
podem necessitar de execução como administrador.*

Como fizemos uso do Docker Compose e de imagens de contêineres disponíveis
publicamente, a execução de toda a pipeline consiste em utilizar um comando para
iniciar os contêineres Docker. Para garantir que não há nenhum container sendo
executado atualmente *e* para limpar dados de execuções anteriores, incluindo
mensagens e a base de dados, utilize:

```bash
$ docker compose down -v
```

Essa limpeza é importante para evitar estados indevidos. Se posteriormente for
desejado reiniciar os containers sem perda de dados, basta remover a flag `-v`
do comando. Caso seja a primeira vez executando, é necessário dar build nas
imagens. Para isso:

```bash
$ docker compose up --build
```

Cao não seja a primeira vez executando e não tenham havido mudanças desde a última
execução, basta utilizar

```bash
$ docker compose up
```

Quando a execução é feita, todos os serviços são levantados e a base de dados Postgres
é criada e alimentada com dados sintéticos. Para verificar que os produtores
estão operando corretamente e que o broker está recebendo transações, é possível:

- Abrir o Kafka UI: http://localhost:8080
- Clicar em "Topics"
- Selecionar tópico: `bank_transactions`
- Ver se há mensagens chegando em tempo real.

Também é possível abrir o `psql` dentro do container, para ter acesso direto ao
banco de dados.

```bash
$ docker exec -it bank_db_postgres psql -U bank_etl -d bank
```

Após levantar os containers Docker, o dashboard poderá ler as tabelas do Postgres
e o Redis. Antes de tudo, para executar o Dashboard, é preciso instalar as
dependências Python dele (de preferência em um ambiente virtual, inicializado
da forma de sua preferência).

```bash
$ pip install -r containers/dashboard/requirements.txt
```

Após instalar, a execução do streamlit é feita pelo seguinte comando:

```bash
$ streamlit run containers/dashboard/dashboard.py
```

O link para acesso no navegador será exibido no terminal.

## Sobre a estrutura dos arquivos

Todos os arquivos críticos para o sistema estão concentrados na pasta `containers`:
cada subpasta corresponde a um serviço diferente. Observe que nem todos os serviços
tem uma pasta correspondente, já que a imagem padrão deles do dockerhub já é
suficiente para a execução dos serviços.

- A subpasta `dashboard` contém o arquivo python do dashboard e suas dependências;

- A subpasta `db-seed` contém o arquivo python que cria as tabelas postgres necessarias
para a execução;

- A subpasta `producer` contém o script python que gera os dados de mock e seus
requerimentos.

- A subpasta `spark-etl` contém o script python correspondente à pipeline spark
e uma pipeline de teste. Também contém os arquivos `jar` necessários para a conexão
do spark com o Postgres, o Redis e o Kafka.

Além dela:

- A pasta `data` contém arquivos de dados que são utilizados no dashboard e que
foram utilizados para testes durante o desenvolvimento;

- A pasta `src` contém arquivos python usados durante o desenvolvimento e que
testam manualmente a pipeline (por meio do comando `spark-submit`) e que populam
o banco de dados.

## Serviços e Portas - Execução local

- **Kafka Broker**: localhost:9092
- **Kafka UI**: localhost:8080
- **PostgreSQL**: localhost:5432
- **Redis**: localhost:6379
- **Streamlit**: localhost:8501

## Troubleshooting

1. **Problemas com Docker:**
   ```bash
   # Parar e remover tudo
   docker compose down -v
   docker system prune -f
   # Essas limpezas garantem que os containers vão ser executados como na primeira execução

   # Reiniciar serviços
   docker compose up
   ```

2. **Verificar logs:**
    ```bash
    # Ver logs específicos do producer
    docker compose logs -f producer
   
    # Ver logs do Kafka
    docker compose logs -f broker

    # Ver logs da pipeline Spark
    docker compose logs -f spark-etl

    # Ver logs do postgres
    docker compose logs -f postgres

    # Ver logs do redis
    docker compose logs -f redis
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

## Estrutura dos Dados de transações

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
