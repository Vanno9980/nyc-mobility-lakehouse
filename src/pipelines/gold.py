from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="gold_daily_mobility",
    comment="Indicatori giornalieri delle corse che superano i controlli Silver."
)
def gold_daily_mobility():
    silver = spark.read.table("silver_yellow_trips")

    return (
        silver
        .groupBy("pickup_date")
        .agg(
            F.count("*").alias("trip_count"),

            F.sum("total_amount").alias("total_amount_sum"),
            F.count("total_amount").alias("total_amount_count"),

            F.sum("trip_distance_miles").alias("distance_miles_sum"),
            F.count("trip_distance_miles").alias("distance_count"),

            F.sum("trip_duration_seconds").alias("duration_seconds_sum"),
            F.count("trip_duration_seconds").alias("duration_count"),

            F.max("_ingested_at").alias("latest_source_ingestion_at")
        )
    )