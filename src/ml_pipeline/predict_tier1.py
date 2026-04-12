import os
from urllib.parse import urlparse
from pyspark.sql import SparkSession
from pyspark.ml.regression import GBTRegressionModel
from pyspark.ml import PipelineModel
from dotenv import load_dotenv

load_dotenv()

# 0. Setup Connection
LOCAL_DATABASE_URL = os.getenv("LOCAL_DATABASE_URL")
parsed = urlparse(LOCAL_DATABASE_URL)
DB_URL = f"jdbc:postgresql://{parsed.hostname}:{parsed.port}{parsed.path}"
PROPS = {
    "user": parsed.username,
    "password": parsed.password,
    "driver": "org.postgresql.Driver"
}

# 1. Initialize Spark
spark = SparkSession.builder \
    .appName("Tier1-Inference") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.5.0") \
    .getOrCreate()

# 2. Load the Preprocessing Pipeline and the Model
# Pipeline handles VectorAssembler AND StandardScaler automatically
preprocessor = PipelineModel.load("/app/models_registry/preprocessing_pipeline")
model = GBTRegressionModel.load("/app/models_registry/tier1_gbt_model")

# 3. Fetch Data from Postgres
# Ensure the table name 'analytics.fct_investment_signals' matches your source data
query = "(SELECT * FROM analytics.fct_investment_signals WHERE trade_date > '2026-04-07') as latest_data"
new_data_df = spark.read.jdbc(url=DB_URL, table=query, properties=PROPS)

# 4. Transform raw data into features (Assembles & Scales)
data_with_features = preprocessor.transform(new_data_df)

# 5. Predict!
predictions = model.transform(data_with_features)

# 6. Show the results
# We include stock_symbol and trade_date so the output actually makes sense
predictions.select("stock_symbol", "trade_date", "prediction").show(10)

# 7. Save results (Optional but recommended)
# predictions.select("stock_symbol", "trade_date", "prediction") \
#     .write.mode("overwrite").parquet("/app/data/predictions/latest_signals")

print("Inference Complete.")