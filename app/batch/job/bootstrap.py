from batch.core import Job
from deps.s3 import s3_upload
from deps.spark import spark_read_s3, spark_read_db, spark_write_db


def _seed_std_addresses():
    local_seed_filepath = "data/hanoi_points_cropped.csv"
    s3_key = "hanoi_points_cropped.csv"
    s3_upload(local_seed_filepath, s3_key)
    df = spark_read_s3("hanoi_points_cropped.csv")
    df = df.withColumnRenamed("osm_id", "std_id")
    df = df.drop("osm_type", "geom_type")
    spark_write_db("std_address", df, "overwrite")
    query = """
        SELECT *
        FROM std_address
    """
    df = spark_read_db(query)
    df.show()


class BootstrapJob(Job):
    def run(self, *args, **kwargs):
        _seed_std_addresses()
