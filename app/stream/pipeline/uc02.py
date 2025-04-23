from pyspark.sql.functions import from_json, col
from pyspark.sql.functions import radians, cos, sin, asin, sqrt, row_number
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
from pyspark.sql.window import Window

from deps.spark import get_spark_session, spark_read_db, spark_write_db


def run():
    spark = get_spark_session()

    # Kafka configuration
    kafka_bootstrap_servers = "kafka:9092"
    kafka_topic = "pgserver.public.students"

    # Define schema for 'before' and 'after'
    student_schema = StructType([
        StructField("address", StringType(), True),
        StructField("longitude", FloatType(), True),
        StructField("latitude", FloatType(), True),
        StructField("student_id", IntegerType(), False),
        StructField("name", StringType(), True)
    ])

    # Define the full schema
    payload_schema = StructType([
        StructField("before", student_schema, True),
        StructField("after", student_schema, True),
    ])

    message_schema = StructType([
        StructField("payload", payload_schema, True)
    ])

    # Read from Kafka
    df_kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "latest") \
        .load()

    # Convert the binary value to string and parse JSON
    df_parsed = df_kafka.selectExpr("CAST(value AS STRING)") \
        .withColumn("data", from_json(col("value"), message_schema)) \
        .select("data.payload.*")

    # Example: Select fields from 'after' part
    df_new_student = df_parsed.select(
        col("after.student_id").alias("student_id"),
        col("after.latitude").alias("student_latitude"),
        col("after.longitude").alias("student_longitude"),
    )

    # Load static bus_stops table
    bus_stops_df = spark_read_db("select * from bus_stops")

    # Cross join each student with all bus stops
    enriched_df = df_new_student.crossJoin(bus_stops_df)

    # Haversine formula to calculate distance between student and bus stop
    R = 6371.0  # Radius of Earth in kilometers

    enriched_df = enriched_df.withColumn("distance", R * 2 * asin(sqrt(
        sin((radians(col("student_latitude")) - radians(col("latitude"))) / 2) ** 2 +
        cos(radians(col("student_latitude"))) * cos(radians(col("latitude"))) *
        sin((radians(col("student_longitude")) - radians(col("longitude"))) / 2) ** 2
    )))

    # Select the closest stop per student
    window_spec = Window.partitionBy("student_id").orderBy(col("distance"))
    nearest_stop_df = enriched_df.withColumn("rank", row_number().over(window_spec)) \
        .filter(col("rank") == 1) \
        .select(col("student_id"), col("stop_id"))

    # Write to assignments table (updating with nearest stop_id per student)
    query = nearest_stop_df.writeStream \
        .foreachBatch(lambda batch_df: spark_write_db("assignments_v2", batch_df, "overwrite")) \
        .start()

    query.awaitTermination()
