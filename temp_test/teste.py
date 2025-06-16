from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, hour

transactions_csv = "data/transacoes_100k.csv"
users_csv = "data/informacoes_cadastro_100k.csv"
regions_csv = "data/regioes_estados_brasil.csv"

spark = SparkSession.builder.appName("bankingETL").getOrCreate()

# transactions = spark.read.option("header", True).csv(transactions_csv).cache()
# users = spark.read.option("header", True).csv(users_csv).cache()
# regions = spark.read.option("header", True).csv(regions_csv).cache()

#Carrega os dados
transactions = spark.read.load(
    transactions_csv,
    format = "csv",
    header = True,
    inferSchema = True
).cache().withColumnRenamed("id_regiao", "id_regiao_t")

users = spark.read.load(
    users_csv,
    format = "csv",
    header = True,
    inferSchema = True
).cache().withColumnRenamed("id_regiao", "id_regiao_u")

regions = spark.read.load(
    regions_csv,
    format = "csv",
    header = True,
    inferSchema = True
).cache()

transactions.show()
transactions.printSchema()
users.show()
users.printSchema()
regions.show(26)
regions.printSchema()

#Faz joins iniciais
transactions_users = transactions.join(
    users,
    transactions["id_usuario_pagador"] == users["id_usuario"],
    how = "left"
)
transactions_users.show()

transactions_users_loc = transactions_users.join(
    regions.withColumnsRenamed({"latitude": "latitude_t",
                                "longitude": "longitude_t",
                                "id_regiao": "id_regiao_t"}),
    on = "id_regiao_t",
    how = "left"
).join(
    regions.withColumnsRenamed(
        {"latitude": "latitude_u",
         "longitude": "longitude_u",
         "id_regiao": "id_regiao_u"}
    ).select(
        col("latitude_u"), col("longitude_u"), col("id_regiao_u")
    ),
    on = "id_regiao_u",
    how = "left"
)
transactions_users_loc.show()

#calcula scores de risco
score_t5 = transactions_users_loc.withColumn(
    "t5_score",
    (((col("latitude_t") - col("latitude_u"))**2) + ((col("longitude_t") - col("longitude_u"))**2))**0.5 #Expressão de coluna
)
score_t5.show()

# Precisa da média, meio paia... deixei um placeholder e joguei o problema pra amanhã
score_t6 = transactions_users_loc.withColumn(
    "t6_score",
    col("longitude_t") * 0
)
score_t6.show()

score_t7 = transactions_users_loc.withColumn(
    "t7_score",
    (hour(col("data_horario")) - 12)/12
)
score_t7.show()

score_medio = transactions.join(
    score_t5.select(col("id_transacao"), col("t5_score")),
    on = "id_transacao",
    how = "inner"
).join(
    score_t6.select(col("id_transacao"), col("t6_score")),
    on = "id_transacao",
    how = "inner"
).join(
    score_t7.select(col("id_transacao"), col("t7_score")),
    on = "id_transacao",
    how = "inner"
).withColumn(
    "score_medio",
    (col("t5_score") + col("t6_score") + col("t7_score"))/3
)
score_medio.show()

# transactions_users.show()
#
# modified = transactions_users.withColumn(
#     "t5_score",
#     col("valor_transacao") - col("limite_PIX") # Expressão de coluna
# )
# modified.show()

spark.stop()


