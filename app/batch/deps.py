import os

from pyspark.sql import SparkSession


def get_spark_session():
    return (SparkSession.builder
            .remote("sc://localhost")
            # .config("spark.jars", "/opt/spark/jars/postgresql-42.2.5.jar")
            .config("spark.hadoop.fs.s3a.access.key", os.getenv("S3_ACCESS_KEY"))
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv("S3_SECRET_KEY"))
            .config("spark.hadoop.fs.s3a.endpoint", os.getenv("S3_ENDPOINT"))
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .getOrCreate())
