import os

from dotenv import load_dotenv

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml import Pipeline
from urllib.parse import urlparse


def run_preprocessing():
    load_dotenv()

    LOCAL_DATABASE_URL = os.getenv("LOCAL_DATABASE_URL")
    parsed = urlparse(LOCAL_DATABASE_URL)
    DB_URL = f"jdbc:postgresql://{parsed.hostname}:{parsed.port}{parsed.path}"
    PROPS = {
        "user": parsed.username,
        "password": parsed.password,
        "driver": "org.postgresql.Driver"
    }

    spark = SparkSession.builder \
        .appName("Preprocessing_Tier1") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.5.0") \
        .getOrCreate()
    
    df = spark.read.jdbc(url=DB_URL, table="analytics.fct_investment_signals", properties=PROPS)

    window_spec = Window.partitionBy("stock_symbol").orderBy("trade_date")
    df = df.withColumn("target", F.lead("daily_change_pct", 1).over(window_spec))
    # df = df.dropna(subset=["target"])

    # feature_cols = ['sma_5', 'sma_20', 'volume_surge_ratio', 'alpha_vs_sector', 'rolling_volatility_20d']
    feature_cols = ['sma_5', 'sma_20', 'volume_surge_ratio', 'rolling_volatility_20d']

    assembler = VectorAssembler(inputCols=feature_cols, outputCol="raw_features", handleInvalid="skip")
    scalar = StandardScaler(inputCol="raw_features", outputCol="features", withStd=True, withMean=True)

    pipeline = Pipeline(stages=[assembler, scalar])

    pipeline_model = pipeline.fit(df)
    preprocessed_df = pipeline_model.transform(df)

    preprocessed_df.write.mode("overwrite").parquet("data/processed/tier1_features.parquet")
    
    cutoff_date = "2026-04-07"

    train_df = preprocessed_df.filter(F.col("target").isNotNull() & (F.col("trade_date") < cutoff_date))
    test_df = preprocessed_df.filter(F.col("trade_date") >= cutoff_date)
    
    train_df.write.mode("overwrite").parquet("data/processed/tier1_train.parquet")
    test_df.write.mode("overwrite").parquet("data/processed/tier1_test.parquet")
    
    # pipeline_model.save("models_registry/preprocessing_pipeline")
    pipeline_model.write().overwrite().save("models_registry/preprocessing_pipeline")
    
    print(f"Preprocessing Complete.")
    print(f"Training records (pre-{cutoff_date}): {train_df.count()}")
    print(f"Testing records (post-{cutoff_date}): {test_df.count()}")


if __name__ == "__main__":
    run_preprocessing()