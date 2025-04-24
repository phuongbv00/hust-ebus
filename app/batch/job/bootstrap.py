import csv
import os
import random

import geopandas as gpd
import pandas as pd
import psycopg
from psycopg import sql
from pyspark.sql import Window
from pyspark.sql.functions import col, udf, row_number, lit, rand
from pyspark.sql.types import FloatType, StringType, IntegerType
from shapely.geometry import LineString, MultiLineString

from batch.core import Job
from deps.biz import get_rand_students, DATABASE_URL
from deps.s3 import s3_upload
from deps.spark import spark_read_s3, spark_write_db, get_spark_session

CREATE_TABLE_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS public.students (
        student_id INTEGER PRIMARY KEY,
        address    TEXT,
        longitude  DOUBLE PRECISION,
        latitude   DOUBLE PRECISION,
        name       TEXT
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS public.roads (
        road_id INTEGER PRIMARY KEY,
        name    TEXT
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS public.road_points (
        road_id   INTEGER,
        longitude DOUBLE PRECISION,
        latitude  DOUBLE PRECISION,
        "order"   INTEGER
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS public.student_clusters (
        cluster_id INTEGER PRIMARY KEY,
        latitude   DOUBLE PRECISION,
        longitude  DOUBLE PRECISION
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS public.bus_stops (
        stop_id   INTEGER PRIMARY KEY,
        road_id   INTEGER,
        latitude  DOUBLE PRECISION,
        longitude DOUBLE PRECISION
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS public.cluster_bus_stop_map (
        stop_id    INTEGER NOT NULL,
        cluster_id INTEGER NOT NULL
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS public.assignments (
        student_id BIGINT PRIMARY KEY,
        stop_id    INTEGER
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS public.buses (
        bus_id    INTEGER PRIMARY KEY,
        latitude  DOUBLE PRECISION,
        longitude DOUBLE PRECISION,
        capacity  INTEGER
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS public.bus_assignments (
        bus_id       BIGINT PRIMARY KEY,
        stop_id      BIGINT,
        distance     DOUBLE PRECISION,
        num_students BIGINT
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS public.student_locations (
        student_id  INT NOT NULL,
        latitude    DOUBLE PRECISION,
        longitude   DOUBLE PRECISION,
        timestamp   TIMESTAMP WITH TIME ZONE NOT NULL
    );
    """,
]

DROP_TABLE_STATEMENTS = [
    "DROP TABLE IF EXISTS public.assignments;",
    "DROP TABLE IF EXISTS public.cluster_bus_stop_map;",
    "DROP TABLE IF EXISTS public.bus_stops;",
    "DROP TABLE IF EXISTS public.student_clusters;",
    "DROP TABLE IF EXISTS public.road_points;",
    "DROP TABLE IF EXISTS public.roads;",
    "DROP TABLE IF EXISTS public.students;",
    "DROP TABLE IF EXISTS public.buses;",
    "DROP TABLE IF EXISTS public.bus_assignments;",
    "DROP TABLE IF EXISTS public.student_locations;",
]


def _create_all_tables():
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            for statement in CREATE_TABLE_STATEMENTS:
                cur.execute(sql.SQL(statement))
        conn.commit()


def _drop_all_tables():
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            for statement in DROP_TABLE_STATEMENTS:
                cur.execute(sql.SQL(statement))
        conn.commit()


def _seed_students():
    spark = get_spark_session()  # must declare to make udf works

    @udf(returnType=StringType())
    def fake_name():
        first_names = [
            'Anh', 'Minh', 'Lan', 'Thu', 'Quyen', 'Mai', 'Hoa', 'Duc', 'Thanh', 'Tuan',
            'Bich', 'Hien', 'Dai', 'Kim', 'Linh', 'Bao', 'Tung', 'Hoai', 'Trinh', 'Vui',
            'Chau', 'Quoc', 'Tam', 'Tram', 'Yen', 'Ha', 'Cuong', 'Hai', 'Vi', 'Tao'
        ]
        last_names = ['Nguyen', 'Tran', 'Le', 'Pham', 'Hoang', 'Ngo', 'Vu', 'Dang', 'Bui', 'Do']
        return f"{random.choice(first_names)} {random.choice(last_names)}"

    local_seed_filepath = f"data/hanoi_points_full.csv"
    s3_key = "hanoi_points.csv"
    s3_upload(local_seed_filepath, s3_key)
    df = spark_read_s3(s3_key) \
        .withColumnRenamed("name", "address") \
        .withColumn("student_id", row_number().over(Window.orderBy(lit(1)))) \
        .withColumn("name", fake_name()) \
        .withColumn("longitude", col("longitude").cast(FloatType())) \
        .withColumn("latitude", col("latitude").cast(FloatType())) \
        .drop("osm_id", "osm_type", "geom_type")
    spark_write_db("students", df)


def _seed_roads():
    # Start Spark session
    spark = get_spark_session()

    # Load GeoJSON
    gdf = gpd.read_file("data/hanoi_roads_full.geojson")

    # Clean and prepare base attributes
    gdf["road_id"] = gdf.get("osm_id", gdf.index)
    gdf["name"] = gdf.get("name", "").fillna("Chưa có tên")

    # --------------------
    # Seed Roads table (road_id, name)
    # --------------------
    roads_pdf = gdf[["road_id", "name"]]
    roads_sdf = spark.createDataFrame(roads_pdf)
    spark_write_db("roads", roads_sdf)

    # --------------------
    # Seed RoadPoints table (road_id, longitude, latitude, order)
    # --------------------

    # Extract (road_id, lon, lat, order) from geometry
    def extract_coords_with_order(r):
        geom = r.geometry
        road_id = r.road_id
        coords = []
        if isinstance(geom, LineString):
            coords = list(geom.coords)
        elif isinstance(geom, MultiLineString):
            for line in geom.geoms:
                coords.extend(line.coords)
        return [(road_id, lon, lat, idx) for idx, (lon, lat) in enumerate(coords)]

    # Explode geometry to points
    coords_data = []
    for _, row in gdf.iterrows():
        coords_data.extend(extract_coords_with_order(row))

    # Create DataFrame
    points_df = pd.DataFrame(coords_data, columns=["road_id", "longitude", "latitude", "order"])
    points_sdf = spark.createDataFrame(points_df)

    # Write to Spark table
    spark_write_db("road_points", points_sdf)


def _export_students_to_csv(limit: int = 150):
    filename = os.path.join("..", "..", f"test/data/students_{limit}.csv")
    os.makedirs(os.path.dirname(filename), exist_ok=True)

    # Call get_rand_students to get random records
    students = get_rand_students(limit)

    # Open .csv data to write
    with open(filename, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)

        writer.writerow(['student_id', 'longitude', 'latitude', 'name', 'address'])

        for student in students:
            writer.writerow([
                student.student_id,
                student.longitude,
                student.latitude,
                student.name,
                student.address
            ])


def _seed_buses(total_bus: int = 20):
    # chọn ngẫu nhiên số chỗ ngồi phổ biến
    @udf(returnType=IntegerType())
    def rand_bus_capacity():
        total_seats = [16, 29, 35, 45]
        return random.choice(total_seats)

    # Import danh sách các xe bus
    local_seed_filepath = f"data/hanoi_points_full.csv"
    s3_key = "hanoi_points.csv"
    s3_upload(local_seed_filepath, s3_key)
    df = spark_read_s3(s3_key) \
        .orderBy(rand()) \
        .limit(total_bus) \
        .withColumn("bus_id", row_number().over(Window.orderBy(lit(1)))) \
        .withColumn("capacity", rand_bus_capacity()) \
        .withColumn("longitude", col("longitude").cast(FloatType())) \
        .withColumn("latitude", col("latitude").cast(FloatType())) \
        .drop("osm_id", "osm_type", "geom_type", "name")
    spark_write_db("buses", df)

    return


class BootstrapJob(Job):
    def run(self, *args, **kwargs):
        _drop_all_tables()
        _create_all_tables()
        _seed_students()
        _seed_roads()
        _seed_buses()
        _export_students_to_csv(50)
        _export_students_to_csv(100)
        _export_students_to_csv(150)
