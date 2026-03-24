from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, sum, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

spark = SparkSession.builder.appName("Task2").getOrCreate()

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

# Convert timestamp
parsed_df = parsed_df.withColumn("event_time", to_timestamp(col("timestamp")))

# Aggregation
agg_df = parsed_df.groupBy("driver_id").agg(
    sum("fare_amount").alias("total_fare"),
    avg("distance_km").alias("avg_distance")
)

# Write each batch to CSV
def write_to_csv(batch_df, batch_id):
    batch_df.write \
        .mode("overwrite") \
        .csv(f"outputs/task_2/batch_{batch_id}")

query = agg_df.writeStream \
    .outputMode("complete") \
    .foreachBatch(write_to_csv) \
    .start()

query.awaitTermination()
