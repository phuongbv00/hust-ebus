import os

from batch.core import Job
from batch.deps import get_spark_session


class JobUC01(Job):
    def run(self, *args, **kwargs):
        print("JobUC01 starting...")
        # Connect to Spark using Spark Connect
        spark = get_spark_session()

        # Demo read file from MinIO
        bucket_name = os.getenv("S3_BUCKET")
        filepath = f"s3a://{bucket_name}/sample.csv"
        df = spark.read.csv(filepath)
        df.show()

        # Demo SQL query
        jdbc_url = f"jdbc:postgresql://{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"

        query = """
            SELECT *
            FROM bus_stop
        """

        df = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("user", os.getenv("DB_USER")) \
            .option("password", os.getenv("DB_PASS")) \
            .option("driver", "org.postgresql.Driver") \
            .option("query", query) \
            .load()

        df.show()

        print("JobUC01 done")
        pass
