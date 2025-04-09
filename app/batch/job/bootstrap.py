from batch.core import Job
from deps.spark import spark_read_s3, spark_read_db


class BootstrapJob(Job):
    def run(self, *args, **kwargs):
        # Demo read file from MinIO
        df = spark_read_s3("sample.csv")
        df.show()

        # Demo SQL query
        query = """
                    SELECT *
                    FROM bus_stop
                """
        df = spark_read_db(query)
        df.show()
