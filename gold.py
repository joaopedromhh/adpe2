from pyspark.sql import SparkSession
import pandas as pd


# nova sessão spark

spark = SparkSession.builder \
    .appName("Delivery_Pipeline_Gold") \
    .getOrCreate()

# puxar dados da camada silver_orders

# pandas para ler parquet (compatível com windows)

pandas_silver = pd.read_parquet("silver_orders_enriched", engine="pyarrow")

# converter colunas
pandas_silver['order_date'] = pandas_silver['order_date'].astype(str)
pandas_silver['first_order_date'] = pandas_silver['first_order_date'].astype(str)

# df em tabela

silver_df = spark.createDataFrame(pandas_silver)

silver_df.createOrReplaceTempView("silver_orders_enriched")


# query

query_gold = """
WITH user_monthly_activity AS (
SELECT user_id, cohort_month, order_month, CAST(MONTHS_BETWEEN(TO_DATE(order_month, 'yyyy-MM'), TO_DATE(cohort_month, 'yyyy-MM')) AS INT) AS month_index
FROM silver_orders_enriched
GROUP BY user_id, cohort_month, order_month
),
cohort_sizes AS (
SELECT cohort_month, COUNT(DISTINCT user_id) AS total_users_in_cohort
FROM user_monthly_activity
WHERE month_index = 0
GROUP BY cohort_month
),
retention_counts AS (
SELECT cohort_month, month_index, COUNT(DISTINCT user_id) AS active_users
FROM user_monthly_activity
GROUP BY cohort_month, month_index
)
SELECT 
r.cohort_month,
s.total_users_in_cohort,
r.month_index,
r.active_users,
ROUND((CAST(r.active_users AS DOUBLE) / s.total_users_in_cohort) * 100, 2) AS retention_percentage
FROM retention_counts AS r
JOIN cohort_sizes AS s ON r.cohort_month = s.cohort_month
ORDER BY r.cohort_month, r.month_index
"""

print("Executando o motor SQL...")

# rodar query
gold_cohort_df = spark.sql(query_gold)

# exibir resultado
gold_cohort_df.show(20)

# tabela agregada pelo pandas
print("Salvando a matriz final em CSV...")
pandas_gold = gold_cohort_df.toPandas()
pandas_gold.to_csv("gold_cohort_matrix.csv", index=False)

print("arquivo gold_cohort_matrix.csv gerado com sucesso!")