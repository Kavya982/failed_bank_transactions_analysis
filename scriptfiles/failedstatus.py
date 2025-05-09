from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize the Spark session
spark = SparkSession.builder.appName("Failed Transaction Filter").getOrCreate()

# Read the cleaned CSV file from Cloud Storage
df = spark.read.csv("gs://failed_transaction_bucket/cleaned_data/cleaned_transactions.csv", header=True, inferSchema=True)

# Assuming there's a 'status' column where 'failed' indicates a failed transaction
failed_transactions = df.filter(col("Status") == "Failed")

# Write the filtered data (failed transactions) back to Cloud Storage
failed_transactions.write.csv("gs://failed_transaction_bucket/failed_transactions_data/failed_transactions.csv", header=True)

# Stop the Spark session
spark.stop()
