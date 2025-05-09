from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create Spark session
spark = SparkSession.builder.appName("CleanBankTransactions").getOrCreate()

# Path to input files in Cloud Storage bucket
input_path = "gs://failed_transaction_bucket/inputfiles/*.csv"  

# Read all CSV files
df = spark.read.option("header", "true").csv(input_path)

# Remove rows with any null or blank values
cleaned_df = df.dropna()
for column in cleaned_df.columns:
    cleaned_df = cleaned_df.filter(col(column) != "")

# Output path for cleaned CSV
output_path = "gs://failed_transaction_bucket/cleaned_data/cleaned_transactions.csv"  

# Save cleaned data to single CSV file
cleaned_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)

print("Cleaning completed. Cleaned CSV file written to:", output_path)

# Stop Spark session
spark.stop()
