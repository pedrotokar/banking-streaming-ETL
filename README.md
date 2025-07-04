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

## Entregas

Relatório: abrir arquivo `Computação Escalável - A2.pdf`

Vídeo: abrir arquivo `nuvem.mp4` ou acessar [este link](https://drive.google.com/file/d/19N7-DmAqbzz6CyVyNHyXFq8sFTOySxEx/view?usp=sharing)

## Como executar localmente

*Observação: a depender das configurações do sistema, os comandos do docker
podem necessitar de execução como administrador.*

Antes de qualquer execução, para acessar a versão mais recente clone o repositório
do GitHub:

```
$ git clone https://github.com/pedrotokar/banking-streaming-ETL.git
```

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

## Como executar em nuvem

A implantação do sistema em nuvem é muito dependente da plataforma em que o deploy
será feito. Este trabalho foi implantado na Amazon Web Services e os maiores detalhes
de quais ferramentas foram utilizadas para o deploy estão no relatório entregue
junto ao trabalho.

Ainda assim, para caso não se tenha acesso à um ambiente onde é possível executar
todo o sistema com o auxílio do Docker Composer, a divisão dos componentes e a
ordem sugerida de implementação é fornecida a seguir. O arquivo `docker-compose.yml`
e as subpastas de `containers` podem ser inspecionados para se obter as imagens
docker necessárias para cada componente.

1. *Banco de dados*: a tabela postgres é dependencia direta tanto da pipeline quanto
dos simuladores de transações, e por isso é recomendado dar deploy do banco em primeiro
lugar. A recomendação é que o banco seja levantado e o script para gerar os dados de
usuários seja executado localmente, já que é necessario executar apenas uma vez;

2. *broker Kafka*: tanto a pipeline quanto os geradores de dados também dependem
do broker. Caso ele seja implementado em um ambiente que não seja especializado
nesse tipo de serviço (como o Amazon MSK), o recomendado é que ele seja implementado
junto com o Zookeeper e o Kafka-ui, para facilitar o desenvolvimento e debug;

3. *Banco de dados Redis*: por ser uma das formas de comunicação entre a pipeline
e o dashboard além do banco Postgres, também é essencial que o Redis esteja
disponível antes da implantação da pipeline;

4. *Pipeline Spark*: com os componentes acima implantados, é possível dar deploy
da pipeline. Antes de implementar o dashboard, é possível debugar ela usando a
UI do Spark.

5. *Dashboard*: com a pipeline em execução, o dashboard pode ler os dados Redis
e Postgres para funcionar. O streamlit, por padrão, expõe o dashboard para acesso
via HTTP.

É possível altera o arquivo `docker-composer.yml` para deixar apenas os componentes
que serão implementados em um ambiente específico, e usar as variáveis de ambiente
para inserir os endereços de IP, portas e outras informações dos serviços implementados.

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
