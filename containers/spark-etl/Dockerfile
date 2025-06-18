FROM jupyter/pyspark-notebook:spark-3.5.0

USER root

# Apenas para garantir que o netcat esteja instalado
RUN apt-get update && apt-get install -y netcat

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY . /app
WORKDIR /app

RUN chmod +x wait-for-kafka.sh

CMD ["./wait-for-kafka.sh"]
