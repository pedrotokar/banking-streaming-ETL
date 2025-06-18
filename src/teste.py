from pyspark.sql import SparkSession
import pyspark.sql.functions as F

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
    # users.select(F.col("id_regiao_u"), F.col("id_usuario")),
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
        F.col("latitude_u"), F.col("longitude_u"), F.col("id_regiao_u")
    ),
    on = "id_regiao_u",
    how = "left"
)
transactions_users_loc.show()

#calcula scores de risco
score_t5 = transactions_users_loc.withColumn(
    "t5_score",
    (((F.col("latitude_t") - F.col("latitude_u"))**2) + ((F.col("longitude_t") - F.col("longitude_u"))**2))**0.5 #Expressão de coluna
)
score_t5.show()

# Precisa da média, meio paia... deixei um placeholder e joguei o problema pra amanhã
score_t6 = transactions_users_loc.withColumn(
    "t6_score",
    F.col("longitude_t") * 0
)
score_t6.show()

score_t7 = transactions_users_loc.withColumn(
    "t7_score",
    (F.hour(F.col("data_horario")) - 12)/12
)
score_t7.show()

mean_score_approved = transactions_users_loc.join(
    score_t5.select(F.col("id_transacao"), F.col("t5_score")),
    on = "id_transacao",
    how = "inner"
).join(
    score_t6.select(F.col("id_transacao"), F.col("t6_score")),
    on = "id_transacao",
    how = "inner"
).join(
    score_t7.select(F.col("id_transacao"), F.col("t7_score")),
    on = "id_transacao",
    how = "inner"
).withColumn(
    "score_medio",
    (F.col("t5_score") + F.col("t6_score") + F.col("t7_score"))/3
).withColumn(
    "score_aprovado",
    F.when(F.col("score_medio") > 6, False).otherwise(True)
)
mean_score_approved.show()

balance_approved = transactions_users.withColumn(
    "saldo_aprovado",
    F.when(F.col("saldo") > F.col("valor_transacao"), True)
    .otherwise(False)
)
balance_approved.show()

limit_approved = transactions_users.withColumn(
    "limite_aprovado",
    F.when(
        F.col("modalidade_pagamento") == "PIX",
        F.when(F.col("valor_transacao") > F.col("limite_PIX"), False).otherwise(True)
    ).when(
        F.col("modalidade_pagamento") == "TED",
        F.when(F.col("valor_transacao") > F.col("limite_TED"), False).otherwise(True)
    ).when(
        F.col("modalidade_pagamento") == "Boleto",
        F.when(F.col("valor_transacao") > F.col("limite_Boleto"), False).otherwise(True)
    ).otherwise(
        F.when(F.col("valor_transacao") > F.col("limite_DOC"), False).otherwise(True)
    )
)
limit_approved.select(F.col("valor_transacao"),
                       F.col("limite_PIX"),
                       F.col("limite_TED"),
                       F.col("limite_DOC"),
                       F.col("limite_boleto"),
                       F.col("modalidade_pagamento"),
                       F.col("limite_aprovado")).show()

transaction_approved = transactions.join(
    mean_score_approved.select(F.col("id_transacao"), F.col("score_aprovado")),
    on = "id_transacao",
    how = "left"
).join(
    balance_approved.select(F.col("id_transacao"), F.col("saldo_aprovado")),
    on = "id_transacao",
    how = "left"
).join(
    limit_approved.select(F.col("id_transacao"), F.col("limite_aprovado")),
    on = "id_transacao",
    how = "left"
).withColumn(
    "transacao_aprovada",
    F.col("score_aprovado") & F.col("saldo_aprovado") & F.col("limite_aprovado")
)

transaction_approved.show()


output = transaction_approved.select(
    F.col("id_transacao"),
    F.col("id_usuario_pagador"),
    F.col("id_usuario_recebedor"),
    F.col("id_regiao_t"),
    F.col("modalidade_pagamento"),
    F.col("data_horario"),
    F.col("valor_transacao"),
    F.col("transacao_aprovada")
)

output.show()

output.write.mode("overwrite").csv("data/output")

spark.stop()


