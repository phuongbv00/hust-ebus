import csv
import os
import random

import geopandas as gpd
import pandas as pd
from pyspark.sql import Window
from pyspark.sql.functions import col, udf, row_number, lit, rand
from pyspark.sql.types import FloatType, StringType, IntegerType
from shapely.geometry import LineString, MultiLineString

from batch.core import Job
from deps.biz import get_rand_students
from deps.s3 import s3_upload
from deps.spark import spark_read_s3, spark_write_db, get_spark_session

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
    spark_write_db("students", df, "overwrite")


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
    spark_write_db("roads", roads_sdf, "overwrite")

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
    spark_write_db("road_points", points_sdf, "overwrite")


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
        total_seats = [16,29,35,45]
        return random.choice(total_seats)

    # Import danh sách các xe bus
    local_seed_filepath = f"data/hanoi_points_full.csv"
    s3_key = "hanoi_points.csv"
    s3_upload(local_seed_filepath, s3_key)
    df = spark_read_s3(s3_key) \
        .orderBy(rand()) \
        .limit(total_bus) \
        .withColumn("bus_id", row_number().over(Window.orderBy(lit(1)))) \
        .withColumn("capacity", rand_bus_capacity() ) \
        .withColumn("longitude", col("longitude").cast(FloatType())) \
        .withColumn("latitude", col("latitude").cast(FloatType())) \
        .drop("osm_id", "osm_type", "geom_type", "name")
    spark_write_db("buses", df, "overwrite")

    return

class BootstrapJob(Job):
    def run(self, *args, **kwargs):
        _seed_students()
        _seed_roads()
        _seed_buses()
        _export_students_to_csv(50)
        _export_students_to_csv(100)
        _export_students_to_csv(150)
