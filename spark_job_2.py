from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys
import shutil
import glob

# Performance alert thresholds based on historical data analysis
NET_IN_THRESHOLD = 3098.77
DISK_IO_THRESHOLD = 4825.84

# Input data files from Kafka consumers and output location
net_csv = "consumed_data/net_data.csv"
disk_csv = "consumed_data/disk_data.csv"
out_csv = "team_128_NET_DISK.csv"

# Configure Spark for optimal performance with 8 partitions
spark = (
    SparkSession.builder.appName("net_disk_alerts_final")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.default.parallelism", "8")
    .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    .getOrCreate()
)

# Load network and disk usage data with proper data types
df_net = (
    spark.read.option("header", True)
    .csv(net_csv)
    .withColumn("net_in", F.col("net_in").cast("double"))
)
df_disk = (
    spark.read.option("header", True)
    .csv(disk_csv)
    .withColumn("disk_io", F.col("disk_io").cast("double"))
)

# Combine network and disk metrics for each server at each timestamp
df = df_net.join(df_disk, on=["ts", "server_id"], how="inner")

# Convert time strings to proper timestamps for window calculations
# We add a dummy date since our data only has time (HH:mm:ss format)
df = df.withColumn(
    "ts_fixed",
    F.to_timestamp(F.concat(F.lit("1970-01-01 "), F.col("ts")), "yyyy-MM-dd HH:mm:ss"),
)
df = df.withColumn("ts_sec", F.unix_timestamp("ts_fixed"))

# Find the time range of our data to create sliding windows
bounds = df.agg(
    F.min("ts_sec").alias("min_ts"), F.max("ts_sec").alias("max_ts")
).collect()[0]

# Exit early if we have no data to process
if bounds["min_ts"] is None:
    print("No data.")
    spark.stop()
    sys.exit(0)

# Set up 30-second sliding windows with 10-second intervals
min_ts, max_ts = int(bounds["min_ts"]), int(bounds["max_ts"])
window_size, slide = 30, 10

# Create all possible window intervals for our time range
starts = list(range(min_ts, max_ts + 1, slide))
windows = [(s, s + window_size) for s in starts]
spark_windows = spark.createDataFrame(windows, ["win_start", "win_end"])

# Match each data point to all windows it belongs to (cross-join approach)
joined = df.crossJoin(spark_windows).filter(
    (F.col("ts_sec") >= F.col("win_start")) & (F.col("ts_sec") < F.col("win_end"))
)

# Calculate maximum values for each server within each window
agg = joined.groupBy("server_id", "win_start", "win_end").agg(
    F.max("net_in").alias("max_net_in"), F.max("disk_io").alias("max_disk_io")
)

# Generate alerts based on threshold violations and format output
result = (
    agg.withColumn(
        "alert",
        F.when(
            (F.col("max_net_in") > NET_IN_THRESHOLD)
            & (F.col("max_disk_io") > DISK_IO_THRESHOLD),
            "Network flood + Disk thrash suspected",
        )
        .when(
            (F.col("max_net_in") > NET_IN_THRESHOLD)
            & (F.col("max_disk_io") <= DISK_IO_THRESHOLD),
            "Possible DDoS",
        )
        .when(
            (F.col("max_disk_io") > DISK_IO_THRESHOLD)
            & (F.col("max_net_in") <= NET_IN_THRESHOLD),
            "Disk thrash suspected",
        )
        .otherwise(""),
    )
    .withColumn("window_start", F.date_format(F.from_unixtime("win_start"), "HH:mm:ss"))
    .withColumn("window_end", F.date_format(F.from_unixtime("win_end"), "HH:mm:ss"))
    .select(
        "server_id",
        "window_start",
        "window_end",
        F.round(F.col("max_net_in"), 2).alias("max_net_in"),
        F.round(F.col("max_disk_io"), 2).alias("max_disk_io"),
        "alert",
    )
    .orderBy("server_id", "window_start")
)

# Save results to a single CSV file (no multiple part files)
result.coalesce(1).write.mode("overwrite").option("header", True).csv("temp_out")
out_file = glob.glob("temp_out/part-*.csv")[0]
shutil.move(out_file, out_csv)
shutil.rmtree("temp_out")

print(f"Wrote to {out_csv}")
spark.stop()
