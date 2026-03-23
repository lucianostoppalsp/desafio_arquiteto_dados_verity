import argparse
from datetime import datetime, timedelta

from pyspark.sql import SparkSession


DEFAULT_DB_HOST = "postgres"
DEFAULT_DB_PORT = "5432"
DEFAULT_DB_NAME = "challenge_db"
DEFAULT_DB_USER = "airflow"
DEFAULT_DB_PASSWORD = "airflow"


def parse_common_args(app_name: str) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=app_name)
    parser.add_argument("--reference-date", required=True, help="Reference date in YYYY-MM-DD format")
    parser.add_argument("--lookback-days", type=int, required=True, help="Number of days to look back")
    parser.add_argument("--db-host", default=DEFAULT_DB_HOST)
    parser.add_argument("--db-port", default=DEFAULT_DB_PORT)
    parser.add_argument("--db-name", default=DEFAULT_DB_NAME)
    parser.add_argument("--db-user", default=DEFAULT_DB_USER)
    parser.add_argument("--db-password", default=DEFAULT_DB_PASSWORD)
    return parser.parse_args()


def compute_window(reference_date: str, lookback_days: int) -> tuple[str, str]:
    end_date = datetime.strptime(reference_date, "%Y-%m-%d").date()
    start_date = end_date - timedelta(days=lookback_days)
    return start_date.isoformat(), end_date.isoformat()


def build_jdbc_url(args: argparse.Namespace) -> str:
    return f"jdbc:postgresql://{args.db_host}:{args.db_port}/{args.db_name}"


def build_jdbc_properties(args: argparse.Namespace) -> dict:
    return {
        "user": args.db_user,
        "password": args.db_password,
        "driver": "org.postgresql.Driver",
    }


def build_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.4")
        .getOrCreate()
    )
