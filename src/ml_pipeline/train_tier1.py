from pyspark.sql import functions as F

from pyspark.sql import SparkSession
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator

# 1. Start Spark
spark = SparkSession.builder.appName("Train_Tier1").getOrCreate()

# 2. Load the pre-split parquets directly
print("Loading pre-processed training and testing sets...")
train_data = spark.read.parquet("data/processed/tier1_train.parquet")
test_data = spark.read.parquet("data/processed/tier1_test.parquet")

# 3. Initialize the Model
gbt = GBTRegressor(featuresCol="features", labelCol="target", maxIter=20)

# 5. Train
print("Training Tier 1 Model...")
model = gbt.fit(train_data)

# 6. Evaluate
predictions = model.transform(test_data)

eval_predictions = predictions.filter(F.col("target").isNotNull())

evaluator = RegressionEvaluator(labelCol="target", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(eval_predictions)

print(f"Root Mean Squared Error (RMSE) on test data: {rmse}")

# 7. Save the model to the registry
model.write().overwrite().save("models_registry/tier1_gbt_model")
print("Model saved successfully to models_registry/tier1_gbt_model")