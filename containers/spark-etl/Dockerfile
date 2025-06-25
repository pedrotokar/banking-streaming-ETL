FROM bitnami/spark:3.5.0

USER root

# Atualizar packages e instalar ferramentas necessarias
RUN apt-get update && \
    apt-get install -y curl wget python3-pip && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Configurar Python
ENV PYTHONPATH="${PYTHONPATH}:/opt/bitnami/spark/python:/opt/bitnami/spark/python/lib/py4j-0.10.9.7-src.zip"
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Instalar dependencias Python com versoes atualizadas para Spark 3.5.0
RUN pip3 install --no-cache-dir --upgrade pip && \
    pip3 install --no-cache-dir \
        kafka-python==2.0.2 \
        py4j==0.10.9.7 \
        pyarrow \
        pandas #Precisa colocar uma versão dps

# Criar diretorio de JARs (caso nao exista)
RUN mkdir -p /opt/bitnami/spark/jars

# Copiar JARs especificos do Spark 3.5.0 (v350)
COPY jars/v350/*.jar /opt/bitnami/spark/jars/

# Copiar JARs adicionais necessarios para o Spark Streaming com Kafka
COPY jars/*.jar /opt/bitnami/spark/jars/

# Verificar se os JARs estão la
RUN ls -1 /opt/bitnami/spark/jars/ | grep -E "spark-sql-kafka|spark-streaming-kafka|kafka-clients|commons-pool2|spark-token-provider"

# Criar diretorios necessarios
RUN mkdir -p /app/data/output /tmp/spark_checkpoint

# Definir permissoes
RUN chown -R 1001:1001 /app /tmp/spark_checkpoint /opt/bitnami/spark/jars && \
    chmod -R 755 /app /tmp/spark_checkpoint

USER 1001
WORKDIR /app

# Healthcheck
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD python3 -c "import pyspark; print('PySpark OK')" || exit 1

# trocar streaming_etl.py para simple_kafka_test.py para testes simples
CMD ["python3", "streaming_etl.py"]
