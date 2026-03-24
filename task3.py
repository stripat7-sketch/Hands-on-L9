from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum, to_timestamp, window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

spark = SparkSession.builder.appName("Task3").getOrCreate()

schema = StructType([
    StructField("trip_id", StringType(), True),
    StructField("driver_id", StringType(), True),
    StructField("distance_km", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

# Read stream
df = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Parse JSON
parsed_df = df.select(from_json(col("value"), schema).alias("data")).select("data.*")

# Convert timestamp + watermark
parsed_df = parsed_df \
    .withColumn("event_time", to_timestamp(col("timestamp"))) \
    .withWatermark("event_time", "1 minute")

# Window aggregation
window_df = parsed_df.groupBy(
    window(col("event_time"), "5 minutes", "1 minute")
).agg(
    sum("fare_amount").alias("total_fare")
)

# Extract window start/end
result_df = window_df.select(
    col("window.start").alias("start_time"),
    col("window.end").alias("end_time"),
    col("total_fare")
)

# Write to CSV
def write_to_csv(batch_df, batch_id):
    batch_df.write \
        .mode("overwrite") \
        .option("header", True) \
        .csv(f"outputs/task_3/batch_{batch_id}")

query = result_df.writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_csv) \
    .start()

query.awaitTermination()
