#!/bin/bash

echo "Esperando Kafka ficar disponível na porta 29092..."

until nc -z broker 29092; do
  echo "Kafka ainda não disponível... aguardando 2s"
  sleep 2
done

echo "Kafka disponível! Iniciando Spark ETL..."

spark-submit \
  --master local[*] \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  streaming_etl.py

