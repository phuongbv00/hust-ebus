import os

from pyspark.sql import SparkSession


def get_spark_session():
    return (SparkSession.builder
            .remote("sc://localhost")
            .getOrCreate())


JDBC_URL = f"jdbc:postgresql://{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"


def spark_read_db(query: str):
    spark = get_spark_session()
    return spark.read \
        .format("jdbc") \
        .option("url", JDBC_URL) \
        .option("user", os.getenv("DB_USER")) \
        .option("password", os.getenv("DB_PASS")) \
        .option("driver", "org.postgresql.Driver") \
        .option("query", query) \
        .load()


def spark_read_s3(key: str):
    spark = get_spark_session()
    bucket_name = os.getenv("S3_BUCKET")
    filepath = f"s3a://{bucket_name}/{key}"
    if key.endswith(".csv"):
        return spark.read.csv(filepath)
    elif key.endswith(".json") or key.endswith(".geojson"):
        return spark.read.json(filepath)
