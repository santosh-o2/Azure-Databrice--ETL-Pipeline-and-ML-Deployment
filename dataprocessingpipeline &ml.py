
df = spark.read.csv("/mnt/blob_storage/large_dataset.csv", header=True, inferSchema=True)
df.show(5)
#pre-process....
df = df.na.fill({"Salary": 0}) 
df = df.filter((df["Age"] >= 18) & (df["Age"] <= 65))
from pyspark.sql.functions import col, lit
df = df.withColumn("targetted_column", col("#") / 1000)
df.show(5)
train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
print(f"Training Rows: {train_df.count()}, Testing Rows: {test_df.count()}")
from pyspark.ml.feature import VectorAssembler
assembler = VectorAssembler(inputCols=["product_name", "customer_location"], outputCol="features")
train_df = assembler.transform(train_df)
from pyspark.ml.regression import LinearRegression
lr = LinearRegression(featuresCol="features", labelCol="Salary")
model = lr.fit(train_df)

print(f"Coefficients: {model.coefficients}, Intercept: {model.intercept}")
test_df = assembler.transform(test_df)
predictions = model.transform(test_df)
from pyspark.ml.evaluation import RegressionEvaluator
evaluator = RegressionEvaluator(labelCol="Salary", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
print(f"Root Mean Square Error (RMSE): {rmse}")
model.save("/mnt/blob_storage/models/linear_model")
print("Model saved successfully!")

from pyspark.ml.regression import LinearRegressionModel
loaded_model = LinearRegressionModel.load("/mnt/blob_storage/models/linear_model")
new_data = spark.read.csv("/mnt/blob_storage/new_data.csv", header=True, inferSchema=True)
new_data = assembler.transform(new_data)
predictions = loaded_model.transform(new_data)
predictions.select("prediction").write.csv("/mnt/blob_storage/output/predictions.csv", header=True)
print("Predictions saved to Azure Blob storage as prediction.csv")
