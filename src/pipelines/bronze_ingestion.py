from pyspark import pipelines as dp
from pyspark.sql import functions as F

landing_path = spark.conf.get("nyc.landing_path")


@dp.table(
    name="bronze_yellow_trips",
    comment="Dati originali Yellow Taxi acquisiti con Auto Loader.",
    table_properties={
        "delta.feature.timestampNtz": "supported"
    }
)
def bronze_yellow_trips():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("pathGlobFilter", "*.parquet")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(landing_path)
        .select(
            "*",
            F.col("_metadata.file_path").alias("_source_file"),
            F.col("_metadata.file_name").alias("_source_file_name"),
            F.current_timestamp().alias("_ingested_at")
        )
        .withColumn(
            "_source_month",
            F.regexp_extract(
                F.col("_source_file_name"),
                r"yellow_tripdata_(\d{4}-\d{2})\.parquet",
                1
            )
        )
    )