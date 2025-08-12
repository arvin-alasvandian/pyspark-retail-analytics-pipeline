import os

import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql import functions as F

from utils import create_spark, resolve_col

BASE = os.path.dirname(os.path.dirname(__file__))
PARQ = os.path.join(BASE, "data", "parquet")
FIG = os.path.join(BASE, "figures")
OUT = os.path.join(BASE, "outputs")
os.makedirs(FIG, exist_ok=True)
os.makedirs(OUT, exist_ok=True)

spark = create_spark("BD02-EDA-SQL")
df3 = spark.read.parquet(os.path.join(PARQ, "df3"))

# Resolve column names
col_purchase_amount = resolve_col(df3, ["PurchaseAmount"])
col_tot_purchases = resolve_col(df3, ["TotalPurchases", "TotalPurchase"])
col_spending = resolve_col(df3, ["SpendingScore"])
col_age = resolve_col(df3, ["Age"])

# ---- Task 4 ----
# Rows removed from df2 -> df3
df2 = spark.read.parquet(os.path.join(PARQ, "df2"))
rows_removed = df2.count() - df3.count()
print(f"Rows removed: {rows_removed}")

# Summary statistics for PurchaseAmount
summary = df3.select(
    F.min(col_purchase_amount).alias("min"),
    F.max(col_purchase_amount).alias("max"),
    F.mean(col_purchase_amount).alias("mean"),
    F.expr(f"percentile_approx({col_purchase_amount}, 0.5)").alias("median"),
    F.variance(col_purchase_amount).alias("variance"),
    F.stddev(col_purchase_amount).alias("stddev"),
).toPandas()
print("Summary stats for PurchaseAmount:")
print(summary)

# Histogram (sample + cap to avoid memory issues)
vals = (
    df3.select(col_purchase_amount)
    .where(F.col(col_purchase_amount).isNotNull())
    .sample(False, 0.5, seed=42)
    .limit(100000)
    .toPandas()[col_purchase_amount]
)
plt.figure()
plt.hist(vals, bins=30)
plt.title("Histogram of PurchaseAmount")
plt.xlabel("PurchaseAmount")
plt.ylabel("Frequency")
plt.savefig(os.path.join(FIG, "purchase_amount_hist.png"), bbox_inches="tight")
plt.close()

# ---- Task 5 ----
# Quartiles for TotalPurchases
quartiles = df3.select(
    F.expr(f"percentile_approx({col_tot_purchases}, array(0.25, 0.5, 0.75))").alias("q")
).toPandas()
q25, q50, q75 = quartiles["q"][0]
print(f"Quartiles for {col_tot_purchases}: Q1={q25}, Median={q50}, Q3={q75}")

# Boxplot for TotalPurchases (sample + cap)
vals_tp = (
    df3.select(col_tot_purchases)
    .where(F.col(col_tot_purchases).isNotNull())
    .sample(False, 0.5, seed=42)
    .limit(100000)
    .toPandas()[col_tot_purchases]
)
plt.figure()
plt.boxplot(vals_tp, vert=True, showfliers=True)
plt.title(f"Boxplot of {col_tot_purchases}")
plt.ylabel(col_tot_purchases)
plt.savefig(os.path.join(FIG, "total_purchases_boxplot.png"), bbox_inches="tight")
plt.close()

# ---- Task 6 ----
# Scatter plot & Pearson correlation
pair_pd = (
    df3.select(col_purchase_amount, col_spending)
    .dropna()
    .sample(False, 0.5, seed=42)
    .limit(100000)
    .toPandas()
)
plt.figure()
plt.scatter(pair_pd[col_spending], pair_pd[col_purchase_amount], s=8, alpha=0.5)
plt.xlabel(col_spending)
plt.ylabel(col_purchase_amount)
plt.title("PurchaseAmount vs SpendingScore")
plt.savefig(os.path.join(FIG, "purchase_vs_spending_scatter.png"), bbox_inches="tight")
plt.close()

corr_val = df3.select(F.corr(col_purchase_amount, col_spending)).first()[0]
print(f"Pearson correlation: {corr_val}")

# ---- Task 7 ----
# Spark SQL query
df3.createOrReplaceTempView("df3")
query = f"""
SELECT {col_age} AS Age, {col_spending} AS SpendingScore
FROM df3
WHERE {col_age} < 50 AND {col_spending} > 100
"""
result = spark.sql(query)
print("SQL query sample:")
result.show(10, truncate=False)
# Save the query result
result.coalesce(1).write.mode("overwrite").option("header", True).csv(
    os.path.join(OUT, "sql_query_result")
)

spark.stop()
