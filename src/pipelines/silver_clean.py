from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    name="silver_yellow_trips",
    comment="Corse Yellow Taxi normalizzate con indicatori di qualità.",
    table_properties={
        "delta.feature.timestampNtz": "supported"
    }
)

@dp.expect_or_drop(
    "valid_timestamps",
    """
    pickup_datetime IS NOT NULL
    AND dropoff_datetime IS NOT NULL
    AND dropoff_datetime > pickup_datetime
    """
)
@dp.expect_or_drop(
    "valid_distance",
    "trip_distance_miles IS NOT NULL AND trip_distance_miles >= 0"
)
@dp.expect_or_drop(
    "valid_locations",
    "pickup_location_id IS NOT NULL AND dropoff_location_id IS NOT NULL"
)

def silver_yellow_trips():
    bronze = spark.readStream.table("bronze_yellow_trips")

    silver = bronze.select(
        F.col("VendorID").alias("vendor_id"),
        F.col("tpep_pickup_datetime").alias("pickup_datetime"),
        F.col("tpep_dropoff_datetime").alias("dropoff_datetime"),
        F.col("passenger_count"),
        F.col("trip_distance").alias("trip_distance_miles"),
        F.col("PULocationID").alias("pickup_location_id"),
        F.col("DOLocationID").alias("dropoff_location_id"),
        F.col("RatecodeID").alias("rate_code_id"),
        F.col("payment_type"),
        F.col("fare_amount"),
        F.col("tip_amount"),
        F.col("total_amount"),
        F.col("_source_file"),
        F.col("_source_file_name"),
        F.col("_source_month"),
        F.col("_ingested_at")
    )

    silver = (
        silver
        .withColumn("pickup_date", F.to_date("pickup_datetime"))
        .withColumn("pickup_hour", F.hour("pickup_datetime"))
        .withColumn(
            "trip_duration_seconds",
            F.expr(
                "timestampdiff(SECOND, pickup_datetime, dropoff_datetime)"
            )
        )
    )

    return silver
