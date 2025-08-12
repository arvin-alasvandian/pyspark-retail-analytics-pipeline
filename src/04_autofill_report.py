import json
import os

from pyspark.sql import functions as F

from utils import create_spark, resolve_col

BASE = os.path.dirname(os.path.dirname(__file__))
PARQ = os.path.join(BASE, "data", "parquet")
OUT = os.path.join(BASE, "outputs")


def main():
    spark = create_spark("AutoFillReport")
    df2 = spark.read.parquet(os.path.join(PARQ, "df2"))
    df3 = spark.read.parquet(os.path.join(PARQ, "df3"))

    # Resolve columns
    col_purchase_amount = resolve_col(df3, ["PurchaseAmount"])
    col_tot_purchases = resolve_col(df3, ["TotalPurchases", "TotalPurchase"])
    col_spending = resolve_col(df3, ["SpendingScore"])

    # Rows removed
    rows_removed = df2.count() - df3.count()

    # Summary stats
    summary_row = df3.select(
        F.min(col_purchase_amount).alias("min"),
        F.max(col_purchase_amount).alias("max"),
        F.mean(col_purchase_amount).alias("mean"),
        F.expr(f"percentile_approx({col_purchase_amount}, 0.5)").alias("median"),
        F.variance(col_purchase_amount).alias("variance"),
        F.stddev(col_purchase_amount).alias("stddev"),
    ).first()

    # Quartiles
    quartiles = (
        df3.select(
            F.expr(f"percentile_approx({col_tot_purchases}, array(0.25,0.5,0.75))").alias("q")
        )
        .first()
        .q
    )
    q1, q2, q3 = quartiles

    # Correlation
    corr = df3.select(F.corr(col_purchase_amount, col_spending)).first()[0]

    # Metrics
    metrics_path = os.path.join(OUT, "metrics.json")
    metrics = {}
    if os.path.exists(metrics_path):
        with open(metrics_path, "r") as f:
            metrics = json.load(f)

    # Simple interpretation helpers
    def corr_strength(v):
        ab = abs(v or 0.0)
        if ab < 0.2:
            return "very weak"
        if ab < 0.4:
            return "weak"
        if ab < 0.6:
            return "moderate"
        if ab < 0.8:
            return "strong"
        return "very strong"

    corr_dir = "positive" if (corr or 0) >= 0 else "negative"

    text = f"""# Findings (auto-filled)

- **Rows removed (Task 4):** {rows_removed}
- **PurchaseAmount summary (Task 4):** min = {summary_row['min']}, max = {summary_row['max']}, mean = {summary_row['mean']}, median = {summary_row['median']}, variance = {summary_row['variance']}, std. dev. = {summary_row['stddev']}.
- **TotalPurchases quartiles (Task 5):** Q1 = {q1}, Median = {q2}, Q3 = {q3}.
- **Correlation (Task 6):** Pearson correlation between PurchaseAmount and SpendingScore = {corr} ({corr_strength(corr)} {corr_dir} linear relationship).

## Model Metrics

- **Decision Tree (Task 8):** {metrics.get('decision_tree', {})}
- **Logistic Regression (Task 9):** {metrics.get('logistic_regression', {})}
- **Linear Regression (Task 10):** {metrics.get('linear_regression', {})}
"""

    with open(os.path.join(BASE, "REPORT_Findings.md"), "w", encoding="utf-8") as f:
        f.write(text)

    spark.stop()


if __name__ == "__main__":
    main()
