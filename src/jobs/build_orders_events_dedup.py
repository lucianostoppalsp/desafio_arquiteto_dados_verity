from pyspark.sql import functions as F
from pyspark.sql.window import Window

from common import build_jdbc_properties, build_jdbc_url, build_spark, compute_window, parse_common_args


if __name__ == "__main__":
    args = parse_common_args("build_orders_events_dedup")
    start_date, end_date = compute_window(args.reference_date, args.lookback_days)

    spark = build_spark("build_orders_events_dedup")
    jdbc_url = build_jdbc_url(args)
    jdbc_properties = build_jdbc_properties(args)

    query = f"""
        (
            SELECT
                event_id,
                order_id,
                customer_id,
                status,
                amount,
                business_date,
                ingested_at,
                source_file
            FROM raw.orders_raw
            WHERE business_date BETWEEN DATE '{start_date}' AND DATE '{end_date}'
        ) AS src
    """

    raw_df = spark.read.jdbc(url=jdbc_url, table=query, properties=jdbc_properties)
    print(f"Raw rows in window [{start_date}, {end_date}]: {raw_df.count()}")

    dedup_window = Window.partitionBy(
        "order_id", "customer_id", "status", "amount", "business_date"
    ).orderBy(F.col("ingested_at").desc(), F.col("event_id").desc())

    dedup_df = (
        raw_df
        .withColumn("rn", F.row_number().over(dedup_window))
        .filter(F.col("rn") == 1)
        .drop("rn")
        .withColumn("processing_ts", F.current_timestamp())
        .select(
            "event_id",
            "order_id",
            "customer_id",
            "status",
            "amount",
            "business_date",
            "ingested_at",
            "source_file",
            "processing_ts",
        )
    )

    print(f"Deduplicated rows to write: {dedup_df.count()}")

    dedup_df.write.jdbc(
        url=jdbc_url,
        table="refined.orders_events_dedup",
        mode="append",
        properties=jdbc_properties,
    )

    spark.stop()
