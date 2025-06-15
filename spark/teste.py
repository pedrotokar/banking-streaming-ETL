from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

transactions_csv = "data/transacoes_100k.csv"
users_csv = "data/informacoes_cadastro_100k.csv"
regions_csv = "data/regioes_estados_brasil.csv"

spark = SparkSession.builder.appName("bankingETL").getOrCreate()

# transactions = spark.read.option("header", True).csv(transactions_csv).cache()
# users = spark.read.option("header", True).csv(users_csv).cache()
# regions = spark.read.option("header", True).csv(regions_csv).cache()

transactions = spark.read.load(transactions_csv,
                               format = "csv",
                               header = True,
                               inferSchema = True).cache()
users = spark.read.load(users_csv,
                        format = "csv",
                        header = True,
                        inferSchema = True).cache()
regions = spark.read.load(regions_csv,
                          format = "csv",
                          header = True,
                          inferSchema = True).cache()

print(transactions.show())
print(users.show())
print(regions.show())

joined = transactions.join(users,
                           transactions["id_usuario_pagador"] == users["id_usuario"],
                           how = "left")

joined.show()
joined = joined.join(regions,
                     on = "id_regiao",
                     how = "left")
joined.show()

def calc_t4_score(linha):
    return 21

modified = joined.withColumn(
    "t4_score",
    col("valor_transacao") - col("limite_PIX") # Expressão de coluna
)
modified.show()

spark.stop()


