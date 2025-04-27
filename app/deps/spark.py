import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession, DataFrame

load_dotenv()

JDBC_URL = f"jdbc:postgresql://{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
DB_DRIVER = "org.postgresql.Driver"


def get_spark_session():
    return (SparkSession.builder
            .remote(f"sc://{os.getenv("SPARK_CONNECT")}")
            .getOrCreate())


def spark_write_db(table: str, df: DataFrame, mode: str = "append"):
    df.write.jdbc(
        url=JDBC_URL,
        table=table,
        mode=mode,
        properties={
            "user": DB_USER,
            "password": DB_PASS,
            "driver": DB_DRIVER,
            "truncate": True,
        }
    )


def spark_read_db(query: str):
    spark = get_spark_session()
    return spark.read \
        .format("jdbc") \
        .option("url", JDBC_URL) \
        .option("user", DB_USER) \
        .option("password", DB_PASS) \
        .option("driver", DB_DRIVER) \
        .option("query", query) \
        .load()


def spark_read_s3(key: str):
    spark = get_spark_session()
    bucket_name = os.getenv("S3_BUCKET")
    filepath = f"s3a://{bucket_name}/{key}"
    if key.endswith(".csv"):
        return spark.read.csv(filepath, header=True)
    else:
        raise RuntimeError(f"Unsupported file type: {key}")
