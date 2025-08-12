import os

from pyspark.sql import functions as F

from utils import create_spark, resolve_col

BASE = os.path.dirname(os.path.dirname(__file__))
DATA = os.path.join(BASE, "data", "customer_purchases.csv")
PARQ = os.path.join(BASE, "data", "parquet")
os.makedirs(PARQ, exist_ok=True)

spark = create_spark("BD02-DataPrep")
df1 = spark.read.option("header", True).option("inferSchema", True).csv(DATA)

print("=== Task 1: Schema ===")
df1.printSchema()
print("Row count:", df1.count())

col_spending = resolve_col(df1, ["SpendingScore"])
col_tot_purchases = resolve_col(df1, ["TotalPurchases", "TotalPurchase"])
col_age = resolve_col(df1, ["Age"])
col_income = resolve_col(df1, ["AnnualIncome"])
col_purchase_amount = resolve_col(df1, ["PurchaseAmount"])

df1_num = (
    df1.withColumn(col_spending, F.col(col_spending).cast("double"))
    .withColumn(col_tot_purchases, F.col(col_tot_purchases).cast("double"))
    .withColumn(col_age, F.col(col_age).cast("double"))
    .withColumn(col_income, F.col(col_income).cast("double"))
    .withColumn(col_purchase_amount, F.col(col_purchase_amount).cast("double"))
)


def median_col(df, col):
    # Compute median excluding zeros (since zeros represent missing values in the brief)
    non_zero = df.where(F.col(col) != 0)
    return non_zero.approxQuantile(col, [0.5], 0.001)[0]


spending_med = median_col(df1_num, col_spending)
totp_med = median_col(df1_num, col_tot_purchases)

df2 = df1_num.withColumn(
    col_spending,
    F.when(F.col(col_spending) == 0, F.lit(spending_med)).otherwise(F.col(col_spending)),
).withColumn(
    col_tot_purchases,
    F.when(F.col(col_tot_purchases) == 0, F.lit(totp_med)).otherwise(F.col(col_tot_purchases)),
)

df3 = df2.where(
    (F.col(col_age) != 0) & (F.col(col_income) != 0) & (F.col(col_purchase_amount) != 0)
)

print("df2 count:", df2.count())
print("df3 count:", df3.count())

df1.write.mode("overwrite").parquet(os.path.join(PARQ, "df1"))
df2.write.mode("overwrite").parquet(os.path.join(PARQ, "df2"))
df3.write.mode("overwrite").parquet(os.path.join(PARQ, "df3"))
spark.stop()
