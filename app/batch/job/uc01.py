import numpy as np
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, radians, sin, cos, asin, sqrt, pow, row_number, monotonically_increasing_id, lit
from pyspark.sql.window import Window
from sklearn.cluster import DBSCAN, KMeans
from sklearn.neighbors import NearestNeighbors

from batch.core import Job
from deps.biz import get_students
from deps.spark import spark_read_db, get_spark_session, spark_write_db

# Define the Earth's radius in meters
EARTH_RADIUS_M = 6_371_000


def haversine_distance(lat1, lng1, lat2, lng2):
    """
    Tính khoảng cách Haversine giữa hai tọa độ địa lý (đơn vị: mét)
    """
    lat1, lng1, lat2, lng2 = map(np.radians, [lat1, lng1, lat2, lng2])
    dlat = lat2 - lat1
    dlng = lng2 - lng1
    a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlng / 2.0) ** 2
    c = 2 * np.arcsin(np.sqrt(a))
    return EARTH_RADIUS_M * c


def find_eps_via_k_distance(x: np.ndarray) -> float:
    """
    Tìm epsilon tối ưu cho DBSCAN bằng biểu đồ k-khoảng cách
    """
    neighbors = NearestNeighbors(n_neighbors=2)
    neighbors_fit = neighbors.fit(x)
    distances, _ = neighbors_fit.kneighbors(x)

    # Sắp xếp khoảng cách tăng dần
    distances = np.sort(distances[:, 1])

    # Xác định điểm 'elbow' (góc khuỷu) bằng ngưỡng lệch chuẩn
    diff = np.diff(distances)
    elbow_point = np.where(diff > np.mean(diff) + np.std(diff))[0][0]
    eps = distances[elbow_point]

    print(f"[INFO] Estimated epsilon from k-distance graph: {eps:.2f}")
    return eps


def get_initial_k_by_dbscan(x: np.ndarray) -> int:
    """
    Ước lượng số lượng cụm ban đầu bằng thuật toán DBSCAN
    """
    eps = find_eps_via_k_distance(x)
    dbscan = DBSCAN(eps=eps, min_samples=2)
    labels = dbscan.fit_predict(x)
    n_clusters = len(set(labels)) - (1 if -1 in labels else 0)

    print(f"[INFO] Initial number of clusters estimated by DBSCAN: {n_clusters}")
    return n_clusters


def is_kmeans_clusters_valid(x: np.ndarray, labels: np.ndarray, centroids: np.ndarray,
                             max_distance_m: float, coverage_ratio: float) -> bool:
    """
    Kiểm tra điều kiện hợp lệ của cụm KMeans:
    Ít nhất {coverage_ratio*100}% số điểm trong mỗi cụm có khoảng cách tới tâm cụm nhỏ hơn max_distance_m.
    """
    for i, center in enumerate(centroids):
        cluster_points = x[labels == i]
        if len(cluster_points) == 0:
            print(f"[WARN] Cluster {i} has no points.")
            return False

        # Tính khoảng cách từ từng điểm trong cụm tới tâm cụm (sử dụng Haversine)
        distances = np.array([
            haversine_distance(p[0], p[1], center[0], center[1])
            for p in cluster_points
        ])

        ratio_within_range = np.sum(distances <= max_distance_m) / len(distances)

        print(f"[INFO] Cluster {i}: {ratio_within_range:.2%} within {max_distance_m} meters")

        if ratio_within_range < coverage_ratio:
            print(
                f"[WARN] Cluster {i} does not meet coverage requirement ({ratio_within_range:.2%} < {coverage_ratio:.2%})")
            return False

    print(f"[INFO] All clusters satisfy the {coverage_ratio:.0%} coverage constraint within {max_distance_m} meters.")
    return True


def find_kmeans_with_walk_constraint(x: np.ndarray, walk_max_distance: float, coverage_ratio: float, max_k: int):
    k_init = get_initial_k_by_dbscan(x)
    print(f"[INFO] Initial k from DBSCAN: {k_init}")
    print("[INFO] Finding k with walk distance constraint...")
    print(f"[INFO] Walk max distance: {walk_max_distance}")
    print(f"[INFO] Coverage ratio: {coverage_ratio}")
    print(f"[INFO] Max k: {max_k}")
    for k in range(k_init, max_k + 1):
        kmeans = KMeans(n_clusters=k, random_state=42, n_init='auto')
        labels = kmeans.fit_predict(x)
        centroids = kmeans.cluster_centers_

        if is_kmeans_clusters_valid(x, labels, centroids, walk_max_distance, coverage_ratio):
            print(f"[INFO] Found valid k = {k}")
            return k, labels, centroids
    raise RuntimeError("No valid k found within max_k limit.")


def find_bus_stops_df(spark: SparkSession, roads_df: DataFrame, cluster_centers: np.ndarray):
    """
        Trả về DataFrame chứa road_id gần nhất cho mỗi cluster (khoảng cách tính theo mét)

        Args:
            spark: SparkSession
            roads_df: Spark DataFrame với cột latitude, longitude
            cluster_centers: np.ndarray với shape (k, 2) gồm (lat, lng)

        Returns:
            Spark DataFrame gồm các cột: stop_id, road_id, latitude, longitude
        """
    # Tạo DataFrame chứa các tâm cụm
    centers_list = [(i, float(center[0]), float(center[1])) for i, center in enumerate(cluster_centers)]
    centers_df = spark.createDataFrame(centers_list, ["cluster_id", "center_lat", "center_lng"])

    # Join roads với cluster centers
    joined_df = roads_df.crossJoin(centers_df)

    # Đổi sang radian
    lat1 = radians(col("latitude"))
    lng1 = radians(col("longitude"))
    lat2 = radians(col("center_lat"))
    lng2 = radians(col("center_lng"))

    dlat = lat2 - lat1
    dlng = lng2 - lng1

    # Haversine formula
    a = pow(sin(dlat / 2), 2) + cos(lat1) * cos(lat2) * pow(sin(dlng / 2), 2)
    c = 2 * asin(sqrt(a))

    with_distance_df = joined_df.withColumn("distance", lit(EARTH_RADIUS_M * c))

    # Tìm road point gần nhất với mỗi cluster
    window = Window.partitionBy("cluster_id").orderBy("distance")
    return with_distance_df.withColumn("rn", row_number().over(window)) \
        .filter(col("rn") == 1) \
        .select("cluster_id", "road_id", "latitude", "longitude") \
        .withColumnRenamed("cluster_id", "stop_id") \
        .withColumn("stop_id", col("stop_id") + lit(1))


class UC01Job(Job):
    def run(self, student_count: int = 100, walk_max_distance: int = 5000, coverage_ratio: float = 0.9,
            max_bus_stop_count: int = 100):
        spark = get_spark_session()

        roads_df = spark_read_db("SELECT * FROM road_points")
        students = get_students(student_count)
        students_coordinates = np.array([[s.latitude, s.longitude] for s in students])

        if len(students) > 0 and roads_df:
            # Thực hiện phân cụm
            k, cluster_labels, cluster_centers = find_kmeans_with_walk_constraint(students_coordinates,
                                                                                  walk_max_distance, coverage_ratio,
                                                                                  max_bus_stop_count)

            # Lưu tạm toạ độ các cụm
            cluster_centers_df = spark.createDataFrame(cluster_centers, ["latitude", "longitude"]) \
                .withColumn("cluster_id", row_number().over(Window.orderBy(lit(1))))
            spark_write_db("student_clusters", cluster_centers_df, "overwrite")

            # Tìm điểm đường gần nhất với mỗi tâm cụm ~ bus stop
            print("[INFO] Bus stops:")
            bus_stops_df = find_bus_stops_df(spark, roads_df, cluster_centers)
            bus_stops_df.show()
            spark_write_db("bus_stops", bus_stops_df, "overwrite")

            # Lưu ma trận (cluster - bus stop)
            cluster_map_bus_stop_df = bus_stops_df.select("stop_id") \
                .withColumn("__row_id", monotonically_increasing_id()) \
                .join(cluster_centers_df.select("cluster_id") \
                      .withColumn("__row_id", monotonically_increasing_id()),
                      on="__row_id") \
                .drop("__row_id")
            spark_write_db("cluster_bus_stop_map", cluster_map_bus_stop_df, "overwrite")

            # Lưu ma trận phân bổ (bus stop - student)
            print("[INFO] Assignments:")
            students_df = spark.createDataFrame([s.student_id for s in students], ["student_id"]) \
                .withColumn("__row_id", monotonically_increasing_id())
            assignments_df = spark.createDataFrame(cluster_labels, ["stop_id"]) \
                .withColumn("stop_id", col("stop_id") + 1) \
                .withColumn("__row_id", monotonically_increasing_id()) \
                .join(students_df, on="__row_id") \
                .drop("__row_id")
            assignments_df.show()
            spark_write_db("assignments", assignments_df, "overwrite")
