from pyspark.sql import functions as F

from common import build_jdbc_properties, build_jdbc_url, build_spark, compute_window, parse_common_args


if __name__ == "__main__":
    args = parse_common_args("build_orders_daily_summary")
    start_date, end_date = compute_window(args.reference_date, args.lookback_days)

    spark = build_spark("build_orders_daily_summary")
    jdbc_url = build_jdbc_url(args)
    jdbc_properties = build_jdbc_properties(args)

    query = f"""
        (
            SELECT
                business_date,
                customer_id,
                total_orders,
                pending_orders,
                paid_orders,
                canceled_orders,
                total_amount,
                pending_amount,
                paid_amount,
                canceled_amount,
                last_ingested_at
            FROM analytics.customer_orders_daily
            WHERE business_date BETWEEN DATE '{start_date}' AND DATE '{end_date}'
        ) AS src
    """

    customer_daily_df = spark.read.jdbc(url=jdbc_url, table=query, properties=jdbc_properties)
    print(f"Customer daily rows in window [{start_date}, {end_date}]: {customer_daily_df.count()}")

    summary_df = (
        customer_daily_df
        .groupBy("business_date")
        .agg(
            F.countDistinct("customer_id").cast("int").alias("total_customers"),
            F.sum("total_orders").cast("int").alias("total_orders"),
            F.sum("pending_orders").cast("int").alias("pending_orders"),
            F.sum("paid_orders").cast("int").alias("paid_orders"),
            F.sum("canceled_orders").cast("int").alias("canceled_orders"),
            F.sum("total_amount").cast("decimal(14,2)").alias("total_amount"),
            F.sum("pending_amount").cast("decimal(14,2)").alias("pending_amount"),
            F.sum("paid_amount").cast("decimal(14,2)").alias("paid_amount"),
            F.sum("canceled_amount").cast("decimal(14,2)").alias("canceled_amount"),
            F.max("last_ingested_at").alias("last_ingested_at"),
        )
        .withColumn("processing_ts", F.current_timestamp())
        .select(
            "business_date",
            "total_customers",
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

    print(f"Daily summary rows to write: {summary_df.count()}")

    summary_df.write.jdbc(
        url=jdbc_url,
        table="analytics.orders_daily_summary",
        mode="append",
        properties=jdbc_properties,
    )

    spark.stop()
