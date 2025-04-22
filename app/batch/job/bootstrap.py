import csv
import os
import random

import geopandas as gpd
import pandas as pd
from pyspark.sql import Window
from pyspark.sql.functions import col, udf, row_number, lit
from pyspark.sql.types import FloatType, StringType
from shapely.geometry import LineString, MultiLineString

from batch.core import Job
from deps.biz import get_rand_students
from deps.s3 import s3_upload
from deps.spark import spark_read_s3, spark_write_db, get_spark_session

# Một số tuyến đường chính (tọa độ gần đúng ở Hà Nội)
road_coords = [
    (21.0285, 105.8542),  # Hồ Hoàn Kiếm
    (21.0370, 105.8342),  # Kim Mã
    (21.0039, 105.8212),  # Nguyễn Trãi
    (21.0297, 105.7925),  # Hồ Tùng Mậu
    (21.0409, 105.7475),  # Lê Trọng Tấn
    (21.0242, 105.8700),  # Minh Khai
    (21.0702, 105.7947),  # Cổ Nhuế
    (21.0062, 105.8418),  # Giải Phóng
    (21.0330, 105.8500),  # Tràng Thi
    (21.0450, 105.8230),  # Xuân Thủy
]

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

# Tạo file buses.csv để lưu vào database
def _export_buses_to_csv(total_buses: int = 10):
    filename = os.path.join("data", f"buses.csv")
    os.makedirs(os.path.dirname(filename), exist_ok=True)

    buses = []
    for i in range(1, total_buses + 1):
        base_lat, base_long = random.choice(road_coords)
        lat = base_lat + random.uniform(-0.001, 0.001)
        long = base_long + random.uniform(-0.001, 0.001)
        buses.append([
            i,
            round(long, 6),
            round(lat, 6),
            random.choice([30, 40, 50, 60])
        ])

    with open(filename, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["bus_id", "longitude", "latitude", "capacity"])
        writer.writerows(buses)


def _seed_buses():
    # Import danh sách các xe bus
    local_seed_filepath = f"data/buses.csv"
    s3_key = "buses.csv"
    s3_upload(local_seed_filepath, s3_key)
    df = spark_read_s3(s3_key) \
        .withColumn("longitude", col("longitude").cast(FloatType())) \
        .withColumn("latitude", col("latitude").cast(FloatType()))
    spark_write_db("buses", df, "overwrite")

    return

class BootstrapJob(Job):
    def run(self, *args, **kwargs):
        _seed_students()
        _seed_roads()
        _export_buses_to_csv(10)
        _seed_buses()
        _export_students_to_csv(50)
        _export_students_to_csv(100)
        _export_students_to_csv(150)
