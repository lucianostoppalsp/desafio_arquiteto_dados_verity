from pyspark.sql import functions as F
from pyspark.sql.window import Window

from common import build_jdbc_properties, build_jdbc_url, build_spark, compute_window, parse_common_args


if __name__ == "__main__":
    args = parse_common_args("build_customer_orders_daily")
    start_date, end_date = compute_window(args.reference_date, args.lookback_days)

    spark = build_spark("build_customer_orders_daily")
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
            FROM refined.orders_events_dedup
            WHERE business_date BETWEEN DATE '{start_date}' AND DATE '{end_date}'
        ) AS src
    """

    dedup_df = spark.read.jdbc(url=jdbc_url, table=query, properties=jdbc_properties)
    print(f"Deduplicated rows in window [{start_date}, {end_date}]: {dedup_df.count()}")

    latest_state_window = Window.partitionBy(
        "order_id", "customer_id", "business_date"
    ).orderBy(F.col("ingested_at").desc(), F.col("event_id").desc())

    latest_state_df = (
        dedup_df
        .withColumn("rn", F.row_number().over(latest_state_window))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    print(f"Latest order states to aggregate: {latest_state_df.count()}")

    customer_daily_df = (
        latest_state_df
        .groupBy("business_date", "customer_id")
        .agg(
            F.countDistinct("order_id").cast("int").alias("total_orders"),
            F.sum(F.when(F.col("status") == "pending", 1).otherwise(0)).cast("int").alias("pending_orders"),
            F.sum(F.when(F.col("status") == "paid", 1).otherwise(0)).cast("int").alias("paid_orders"),
            F.sum(F.when(F.col("status") == "canceled", 1).otherwise(0)).cast("int").alias("canceled_orders"),
            F.sum(F.col("amount")).cast("decimal(14,2)").alias("total_amount"),
            F.sum(F.when(F.col("status") == "pending", F.col("amount")).otherwise(F.lit(0))).cast("decimal(14,2)").alias("pending_amount"),
            F.sum(F.when(F.col("status") == "paid", F.col("amount")).otherwise(F.lit(0))).cast("decimal(14,2)").alias("paid_amount"),
            F.sum(F.when(F.col("status") == "canceled", F.col("amount")).otherwise(F.lit(0))).cast("decimal(14,2)").alias("canceled_amount"),
            F.max("ingested_at").alias("last_ingested_at"),
        )
        .withColumn("processing_ts", F.current_timestamp())
        .select(
            "business_date",
            "customer_id",
            "total_orders",
            "pending_orders",
            "paid_orders",
            "canceled_orders",
            "total_amount",
            "pending_amount",
            "paid_amount",
            "canceled_amount",
            "last_ingested_at",
            "processing_ts",
        )
    )

    print(f"Customer daily rows to write: {customer_daily_df.count()}")

    customer_daily_df.write.jdbc(
        url=jdbc_url,
        table="analytics.customer_orders_daily",
        mode="append",
        properties=jdbc_properties,
    )

    spark.stop()
