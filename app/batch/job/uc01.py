import os

from batch.core import Job
from batch.deps import get_spark_session


class JobUC01(Job):
    def run(self, *args, **kwargs):
        print("JobUC01 starting...")
        # Connect to Spark using Spark Connect
        spark = get_spark_session()
        bucket_name = os.getenv("S3_BUCKET")
        filepath = f"s3a://{bucket_name}/sample.csv"
        df = spark.read.csv(filepath)
        df.show()
        print("JobUC01 done")
        pass
