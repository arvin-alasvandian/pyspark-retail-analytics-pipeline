import os, json
from pyspark.sql import functions as F
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.classification import DecisionTreeClassifier, LogisticRegression
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator, RegressionEvaluator
from utils import create_spark, resolve_col

BASE = os.path.dirname(os.path.dirname(__file__))
PARQ = os.path.join(BASE, "data", "parquet")
OUT = os.path.join(BASE, "outputs")
os.makedirs(OUT, exist_ok=True)

spark = create_spark("BD02-Models")
df3 = spark.read.parquet(os.path.join(PARQ, "df3"))

# Resolve columns
col_age = resolve_col(df3, ["Age"])
col_gender = resolve_col(df3, ["Gender"])
col_income = resolve_col(df3, ["AnnualIncome"])
col_spending = resolve_col(df3, ["SpendingScore"])
col_category = resolve_col(df3, ["PurchaseCategory"])
col_total_purchases = resolve_col(df3, ["TotalPurchases", "TotalPurchase"])
col_purchase_amount = resolve_col(df3, ["PurchaseAmount"])
col_outcome = resolve_col(df3, ["Outcome"])

# Cast numeric types
for c in [col_age, col_income, col_spending, col_total_purchases, col_purchase_amount, col_outcome]:
    df3 = df3.withColumn(c, F.col(c).cast("double"))

# Encode categoricals
indexers = [
    StringIndexer(inputCol=col_gender, outputCol=f"{col_gender}_idx", handleInvalid="keep"),
    StringIndexer(inputCol=col_category, outputCol=f"{col_category}_idx", handleInvalid="keep")
]
encoders = [
    OneHotEncoder(inputCols=[f"{col_gender}_idx", f"{col_category}_idx"],
                  outputCols=[f"{col_gender}_oh", f"{col_category}_oh"])
]

# Feature vector for classifiers
feature_cols = [col_age, col_income, col_spending, col_total_purchases, f"{col_gender}_oh", f"{col_category}_oh"]
assembler_cls = VectorAssembler(inputCols=feature_cols, outputCol="features", handleInvalid="keep")

# Split
train, test = df3.randomSplit([0.8, 0.2], seed=42)

# ---- Task 8: Decision Tree Classifier ----
dt = DecisionTreeClassifier(labelCol=col_outcome, featuresCol="features", maxDepth=5, seed=42)
pipeline_dt = Pipeline(stages=indexers + encoders + [assembler_cls, dt])
model_dt = pipeline_dt.fit(train)
pred_dt = model_dt.transform(test)

acc_eval = MulticlassClassificationEvaluator(labelCol=col_outcome, predictionCol="prediction", metricName="accuracy")
f1_eval = MulticlassClassificationEvaluator(labelCol=col_outcome, predictionCol="prediction", metricName="f1")
auroc_eval = BinaryClassificationEvaluator(labelCol=col_outcome, rawPredictionCol="rawPrediction", metricName="areaUnderROC")

dt_metrics = {
    "accuracy": float(acc_eval.evaluate(pred_dt)),
    "f1": float(f1_eval.evaluate(pred_dt)),
    "auroc": float(auroc_eval.evaluate(pred_dt))
}
print("Decision Tree metrics:", dt_metrics)

# ---- Task 9: Logistic Regression ----
lr = LogisticRegression(labelCol=col_outcome, featuresCol="features", maxIter=50)
pipeline_lr = Pipeline(stages=indexers + encoders + [assembler_cls, lr])
model_lr = pipeline_lr.fit(train)
pred_lr = model_lr.transform(test)

lr_metrics = {
    "accuracy": float(acc_eval.evaluate(pred_lr)),
    "f1": float(f1_eval.evaluate(pred_lr)),
    "auroc": float(auroc_eval.evaluate(pred_lr))
}
print("Logistic Regression metrics:", lr_metrics)

# ---- Task 10: Linear Regression (PurchaseAmount ~ AnnualIncome) ----
assembler_reg = VectorAssembler(inputCols=[col_income], outputCol="features")
linreg = LinearRegression(featuresCol="features", labelCol=col_purchase_amount)
pipeline_reg = Pipeline(stages=[assembler_reg, linreg])
model_reg = pipeline_reg.fit(train.select(col_income, col_purchase_amount).na.drop())
pred_reg = model_reg.transform(test.select(col_income, col_purchase_amount).na.drop())

reg_eval_rmse = RegressionEvaluator(labelCol=col_purchase_amount, predictionCol="prediction", metricName="rmse")
reg_eval_r2 = RegressionEvaluator(labelCol=col_purchase_amount, predictionCol="prediction", metricName="r2")
reg_metrics = {
    "rmse": float(reg_eval_rmse.evaluate(pred_reg)),
    "r2": float(reg_eval_r2.evaluate(pred_reg))
}
print("Linear Regression metrics:", reg_metrics)

# Save metrics
with open(os.path.join(OUT, "metrics.json"), "w") as f:
    json.dump({"decision_tree": dt_metrics, "logistic_regression": lr_metrics, "linear_regression": reg_metrics}, f, indent=2)

spark.stop()
