from pyspark.sql import SparkSession

def create_spark(app_name="CustomerPurchasesApp"):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def resolve_col(df, options):
    for c in options:
        if c in df.columns:
            return c
    raise ValueError(f"None of {options} found in DataFrame.")
