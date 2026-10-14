from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min, date_format
from pyspark.sql.window import Window

# iniciar sessão Spark
spark = SparkSession.builder \
    .appName("Delivery_Pipeline_Silver") \
    .getOrCreate()

# ler dados
# uso do inferschema para pegar dados da coluna | não recomendado para ambientes de produção, mas útil para estudos e prototipagem.

users_df = spark.read.csv("users_bronze.csv", header=True, inferSchema=True)
orders_df = spark.read.csv("orders_bronze.csv", header=True, inferSchema=True)

# limpar dados

# limpar dados nulos

orders_clean = orders_df \
    .filter(col('status') == 'completed') \
    .dropna(subset=['total_amount'])

# cruzar dados limpos (novo df) com usuários

silver_df = orders_clean.join(users_df, on = "user_id", how = "inner")


# descobrir a data da PRIMEIRA compra de cada usuário.
# Window Function (particionando por usuário) para olhar o histórico dele.

window_spec = Window.partitionBy("user_id")

silver_enriched = silver_df \
    .withColumn("first_order_date", min("order_date").over(window_spec))

# Agora, formatamos as datas para 'YYYY-MM' para facilitar o agrupamento analítico
silver_enriched = silver_enriched \
    .withColumn("order_month", date_format(col("order_date"), "yyyy-MM")) \
    .withColumn("cohort_month", date_format(col("first_order_date"), "yyyy-MM"))


# salvar em Parquet (formato colunar otimizado para analítica) 
# particionar por order_month para otimizar consultas futuras


# verificar linhas
silver_enriched.show(5)

# Mostra quantas linhas existem na tabela
print(f"Total de linhas na camada Silver: {silver_enriched.count()}")

# salvar através do pandas para compatibilidade com windows

pandas_df = silver_enriched.toPandas()

#manter estrutura de parquet e particionar por order_month

pandas_df.to_parquet(
    "silver_orders_enriched",
    engine = "pyarrow",
    partition_cols = ["order_month"]
)

print("camada silver salva com sucesso em formato Parquet, particionada por order_month.")