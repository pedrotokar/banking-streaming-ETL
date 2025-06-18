# containers/spark-etl/test_spark.py
# Script para testar se o ambiente Spark está funcionando

import sys
import os

print("=== Teste do Ambiente Spark ===")
print(f"Python version: {sys.version}")
print(f"Python path: {sys.path}")

try:
    print("\n1. Testando importação do PySpark...")
    from pyspark.sql import SparkSession
    print("✓ PySpark importado com sucesso!")
    
    print("\n2. Testando criação do SparkSession...")
    spark = SparkSession.builder \
        .appName("TestApp") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
        .getOrCreate()
    print("✓ SparkSession criado com sucesso!")
    
    print("\n3. Testando funcionalidade básica...")
    df = spark.range(10).toDF("number")
    count = df.count()
    print(f"✓ DataFrame criado e contado: {count} registros")
    
    print("\n4. Testando conectores Kafka...")
    try:
        # Tentar registrar o formato Kafka
        spark.readStream.format("kafka")
        print("✓ Conector Kafka disponível!")
    except Exception as e:
        print(f"✗ Erro no conector Kafka: {e}")
    
    print("\n5. Testando kafka-python...")
    try:
        import kafka
        print(f"✓ kafka-python versão: {kafka.__version__}")
    except Exception as e:
        print(f"✗ Erro no kafka-python: {e}")
    
    print("\n6. Listando JARs do Spark...")
    jars_dir = "/opt/bitnami/spark/jars"
    if os.path.exists(jars_dir):
        kafka_jars = [f for f in os.listdir(jars_dir) if 'kafka' in f.lower()]
        if kafka_jars:
            print("✓ JARs Kafka encontrados:")
            for jar in kafka_jars:
                print(f"  - {jar}")
        else:
            print("✗ Nenhum JAR Kafka encontrado")
    
    spark.stop()
    print("\n=== Todos os testes concluídos! ===")
    
except Exception as e:
    print(f"✗ Erro durante o teste: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)