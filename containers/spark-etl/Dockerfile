# containers/spark-etl/Dockerfile - Versão robusta
FROM bitnami/spark:3.4.1

USER root

# Atualizar packages e instalar ferramentas necessárias
RUN apt-get update && \
    apt-get install -y curl wget python3-pip && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Configurar Python
ENV PYTHONPATH="${PYTHONPATH}:/opt/bitnami/spark/python:/opt/bitnami/spark/python/lib/py4j-0.10.9.7-src.zip"
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Instalar dependências Python com versões específicas
RUN pip3 install --no-cache-dir --upgrade pip && \
    pip3 install --no-cache-dir \
        kafka-python==2.0.2 \
        py4j==0.10.9.7

# Criar diretório para JARs se não existir
RUN mkdir -p /opt/bitnami/spark/jars

# Baixar os JARs necessários para Kafka (versão completa e compatível)
RUN wget --retry-connrefused --waitretry=1 --read-timeout=20 --timeout=15 -t 3 \
    -O /opt/bitnami/spark/jars/spark-sql-kafka-0-10_2.12-3.4.1.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.4.1/spark-sql-kafka-0-10_2.12-3.4.1.jar && \
    echo "Downloaded spark-sql-kafka JAR successfully"

RUN wget --retry-connrefused --waitretry=1 --read-timeout=20 --timeout=15 -t 3 \
    -O /opt/bitnami/spark/jars/kafka-clients-3.4.0.jar \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar && \
    echo "Downloaded kafka-clients JAR successfully"

RUN wget --retry-connrefused --waitretry=1 --read-timeout=20 --timeout=15 -t 3 \
    -O /opt/bitnami/spark/jars/spark-streaming-kafka-0-10_2.12-3.4.1.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-streaming-kafka-0-10_2.12/3.4.1/spark-streaming-kafka-0-10_2.12-3.4.1.jar && \
    echo "Downloaded spark-streaming-kafka JAR successfully"

RUN wget --retry-connrefused --waitretry=1 --read-timeout=20 --timeout=15 -t 3 \
    -O /opt/bitnami/spark/jars/commons-pool2-2.11.1.jar \
    https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar && \
    echo "Downloaded commons-pool2 JAR successfully"

# Baixar dependências adicionais que podem estar faltando
RUN wget --retry-connrefused --waitretry=1 --read-timeout=20 --timeout=15 -t 3 \
    -O /opt/bitnami/spark/jars/spark-token-provider-kafka-0-10_2.12-3.4.1.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.4.1/spark-token-provider-kafka-0-10_2.12-3.4.1.jar && \
    echo "Downloaded spark-token-provider-kafka JAR successfully"

# Verificar se os JARs foram baixados corretamente
RUN ls -la /opt/bitnami/spark/jars/ | grep -E "(kafka|commons-pool2)"

# Criar diretórios necessários
RUN mkdir -p /app/data/output && \
    mkdir -p /tmp/spark_checkpoint

# Definir permissões
RUN chown -R 1001:1001 /app /tmp/spark_checkpoint /opt/bitnami/spark/jars && \
    chmod -R 755 /app /tmp/spark_checkpoint

USER 1001

WORKDIR /app

# Adicionar um healthcheck
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD python3 -c "import pyspark; print('PySpark OK')" || exit 1

CMD ["python3", "simple_kafka_test.py"]