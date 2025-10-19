#!/usr/bin/env python3

"""Spark job to compute network/disk windowed maxima and alerts, and write CSV output."""

import os
import sys
import glob
import shutil

# IMPORTANT: Update these thresholds with YOUR assigned values
NET_IN_THRESHOLD = 3098.77
DISK_IO_THRESHOLD = 4825.84

TEAM_NUMBER = "128"  # Replace with your team number


def _ensure_spark_python_on_path():
    """Ensure we use the Spark installation's Python (pyspark + py4j).

    This avoids mismatches with a venv's pyspark that doesn't match the local Spark.
    """
    if "SPARK_HOME" not in os.environ:
        os.environ["SPARK_HOME"] = "/opt/spark"
    spark_home = os.environ["SPARK_HOME"]
    spark_python = os.path.join(spark_home, "python")
    if os.path.isdir(spark_python) and spark_python not in sys.path:
        sys.path.insert(0, spark_python)
    # Add the py4j zip from the Spark python lib directory (if present)
    py4j_glob = os.path.join(spark_python, "lib", "py4j-*.zip")
    py4j_zips = glob.glob(py4j_glob)
    if py4j_zips and py4j_zips[0] not in sys.path:
        sys.path.insert(0, py4j_zips[0])


def process_net_disk_data():
    # Defer PySpark imports until after sys.path is adjusted
    _ensure_spark_python_on_path()

    from pyspark.sql import SparkSession  # noqa: E402
    from pyspark.sql.functions import (  # noqa: E402
        col,
        max as spark_max,
        window,
        from_unixtime,
        date_format,
        round as spark_round,
        udf,
    )
    from pyspark.sql.types import StringType, TimestampType  # noqa: E402

    # Initialize Spark Session
    spark = (
        SparkSession.builder.appName("Network_Disk_Analysis")
        .master("local[*]")
        .getOrCreate()
    )

    # Read CSV files
    net_df = spark.read.csv("net_data.csv", header=True, inferSchema=True)
    disk_df = spark.read.csv("disk_data.csv", header=True, inferSchema=True)

    # Convert timestamp to TimestampType.
    # If `ts` is numeric (epoch seconds) use from_unixtime; if it is already TIMESTAMP, just reuse it.
    def ensure_timestamp(df):
        ts_field = next((f for f in df.schema.fields if f.name == "ts"), None)
        if ts_field is not None and isinstance(ts_field.dataType, TimestampType):
            return df.withColumn("timestamp", col("ts"))
        return df.withColumn(
            "timestamp",
            from_unixtime(col("ts").cast("long")).cast(TimestampType()),
        )

    net_df = ensure_timestamp(net_df)
    disk_df = ensure_timestamp(disk_df)

    # Join Network and Disk data on timestamp and server_id
    joined_df = net_df.join(disk_df, ["ts", "server_id", "timestamp"], "inner")

    # Window-based aggregation (30-second window, 10-second slide)
    windowed_df = joined_df.groupBy(
        col("server_id"), window(col("timestamp"), "30 seconds", "10 seconds")
    ).agg(
        spark_max("net_in").alias("max_net_in"),
        spark_max("disk_io").alias("max_disk_io"),
    )

    # Format window times
    result_df = windowed_df.select(
        col("server_id"),
        date_format(col("window.start"), "HH:mm:ss").alias("window_start"),
        date_format(col("window.end"), "HH:mm:ss").alias("window_end"),
        col("max_net_in"),
        col("max_disk_io"),
    )

    # Apply alerting logic
    def determine_alert(max_net_in, max_disk_io):
        if max_net_in > NET_IN_THRESHOLD and max_disk_io > DISK_IO_THRESHOLD:
            return "Network flood + Disk thrash suspected"
        elif max_net_in > NET_IN_THRESHOLD and max_disk_io <= DISK_IO_THRESHOLD:
            return "Possible DDoS"
        elif max_disk_io > DISK_IO_THRESHOLD and max_net_in <= NET_IN_THRESHOLD:
            return "Disk thrash suspected"
        else:
            return "Normal"

    # Register UDF
    alert_udf = udf(determine_alert, StringType())

    result_df = result_df.withColumn(
        "alert", alert_udf(col("max_net_in"), col("max_disk_io"))
    )

    # Format numeric columns to 2 decimal places using Spark functions
    result_df = result_df.withColumn("max_net_in", spark_round(col("max_net_in"), 2))
    result_df = result_df.withColumn("max_disk_io", spark_round(col("max_disk_io"), 2))

    result_df = result_df.filter(
        date_format(col("window.start"), "HH:mm:ss") >= "20:53:00"
    )

    # Save to a single CSV file using Spark (write to a temp folder then move the part file)
    tmp_out_dir = f"tmp_team_{TEAM_NUMBER}_net_disk"
    result_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(
        tmp_out_dir
    )

    # Find the produced part file and rename it
    part_files = glob.glob(os.path.join(tmp_out_dir, "part-*.csv"))
    if part_files:
        output_file = f"team_{TEAM_NUMBER}_NET_DISK.csv"
        shutil.move(part_files[0], output_file)
        # remove the temporary folder
        try:
            shutil.rmtree(tmp_out_dir)
        except Exception:
            pass
        print("Results saved to {}".format(output_file))
    else:
        print("No part CSV file produced in {}".format(tmp_out_dir))

    print("\nSample results:")
    result_df.show(10, truncate=False)

    # Show alert statistics
    print("\nAlert Distribution:")
    result_df.groupBy("alert").count().show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    process_net_disk_data()
