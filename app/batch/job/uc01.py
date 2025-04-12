import matplotlib.pyplot as plt
import numpy as np
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, radians, sin, cos, asin, sqrt, pow, row_number, monotonically_increasing_id
from pyspark.sql.window import Window
from scipy.spatial.distance import euclidean
from sklearn.cluster import DBSCAN, KMeans
from sklearn.preprocessing import StandardScaler

from batch.core import Job
from deps.biz import get_rand_students
from deps.spark import spark_read_db, get_spark_session, spark_write_db


def find_optimal_eps(X: np.ndarray) -> float:
    """Tìm epsilon tối ưu cho DBSCAN bằng k-distance graph"""
    from sklearn.neighbors import NearestNeighbors

    neighbors = NearestNeighbors(n_neighbors=2)
    neighbors_fit = neighbors.fit(X)
    distances, _ = neighbors_fit.kneighbors(X)

    # Sắp xếp khoảng cách để vẽ k-distance graph
    distances = np.sort(distances[:, 1])

    # Vẽ k-distance graph
    plt.figure(figsize=(10, 6))
    plt.plot(range(len(distances)), distances)
    plt.xlabel('Points')
    plt.ylabel('k-distance')
    plt.title('k-distance graph')
    # TODO
    # plt.savefig('k_distance_graph.png')
    plt.close()

    # Tìm "elbow point" bằng phương pháp đơn giản
    diff = np.diff(distances)
    elbow_point = np.where(diff > np.mean(diff) + np.std(diff))[0][0]

    return distances[elbow_point]


def determine_k_with_dbscan(X: np.ndarray) -> int:
    """Xác định số cluster K bằng DBSCAN"""
    # Tìm epsilon tối ưu
    eps = find_optimal_eps(X)

    # Chạy DBSCAN
    dbscan = DBSCAN(eps=eps, min_samples=2)
    clusters = dbscan.fit_predict(X)

    # Số lượng cluster (không tính noise points là -1)
    n_clusters = len(set(clusters)) - (1 if -1 in clusters else 0)

    return n_clusters


def print_cluster_coordinates(cluster_centers: np.ndarray,
                              cluster_labels: np.ndarray,
                              students: np.ndarray):
    """
    In ra tọa độ trung tâm của từng cụm và thông tin khoảng cách
    """
    # Với mỗi cụm
    for cluster_id in range(len(cluster_centers)):
        # Lấy học sinh trong cụm
        cluster_students = students[cluster_labels == cluster_id]

        # Tính trung tâm thực tế của cụm từ vị trí học sinh
        cluster_center = np.mean([[s.latitude, s.longitude] for s in cluster_students], axis=0)

        print(f"\nCụm {cluster_id + 1}:")
        print(f"Tọa độ trung tâm: ({cluster_center[0]:.6f}, {cluster_center[1]:.6f})")
        print(f"Số học sinh trong cụm: {len(cluster_students)}")

        # Tính khoảng cách từ tâm đến mỗi học sinh
        max_distance = 0
        furthest_student = None
        distances = []

        for student in cluster_students:
            student_coords = np.array([student.latitude, student.longitude])
            distance = euclidean(cluster_center, student_coords)
            distances.append((student, distance))

            if distance > max_distance:
                max_distance = distance
                furthest_student = student

        # In thông tin khoảng cách
        print("\nKhoảng cách từ tâm đến các học sinh:")
        for student, distance in sorted(distances, key=lambda x: x[1], reverse=True):
            print(f"- {student.name}: {distance:.6f} km")
            print(f"  Tọa độ: ({student.latitude:.6f}, {student.longitude:.6f})")

        print(f"\nHọc sinh xa nhất trong cụm:")
        print(f"- Tên: {furthest_student.name}")
        print(f"- Tọa độ: ({furthest_student.latitude:.6f}, {furthest_student.longitude:.6f})")
        print(f"- Khoảng cách: {max_distance:.6f} km")
        print("-" * 50)


def cluster_students(students: np.ndarray) -> tuple[np.ndarray, int, np.ndarray]:
    """Hàm chính để phân cụm học sinh"""
    # Chuẩn bị dữ liệu
    coords = np.array([[s.latitude, s.longitude] for s in students])
    scaler = StandardScaler()
    x = scaler.fit_transform(coords)

    # Xác định K bằng DBSCAN
    k = determine_k_with_dbscan(x)
    print(f"Số cụm được xác định bởi DBSCAN: {k}")

    # Thực hiện K-means
    kmeans = KMeans(n_clusters=k, random_state=42)
    cluster_labels = kmeans.fit_predict(x)

    # Chuyển tâm cụm về toạ độ thực
    cluster_centers = scaler.inverse_transform(kmeans.cluster_centers_)

    # In thông tin về các cụm
    print("\nThông tin chi tiết về các cụm:")
    print_cluster_coordinates(cluster_centers, cluster_labels, students)

    return cluster_labels, k, cluster_centers


def find_bus_stops_df(spark: SparkSession, roads_df: DataFrame, cluster_centers: np.ndarray):
    """
        Trả về DataFrame chứa road_id gần nhất cho mỗi cluster (khoảng cách tính theo mét)

        Args:
            spark: SparkSession
            roads_df: Spark DataFrame với cột latitude, longitude
            cluster_centers: np.ndarray với shape (k, 2) gồm (lat, lng)

        Returns:
            Spark DataFrame gồm các cột: cluster_id, road_id, latitude, longitude, distance (meters)
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
    earth_r = 6371000.0  # bán kính Trái Đất (m)

    with_distance_df = joined_df.withColumn("distance", earth_r * c)

    # Tìm road point gần nhất với mỗi cluster
    window = Window.partitionBy("cluster_id").orderBy("distance")
    return with_distance_df.withColumn("rn", row_number().over(window)) \
        .filter(col("rn") == 1) \
        .select("cluster_id", "road_id", "latitude", "longitude") \
        .withColumnRenamed("cluster_id", "stop_id") \
        .withColumn("stop_id", col("stop_id") + 1)


class UC01Job(Job):
    def run(self, size=50, *args, **kwargs):
        spark = get_spark_session()

        roads_df = spark_read_db("SELECT * FROM road_points")
        students = np.array(get_rand_students(size))

        if students.size > 0 and roads_df:
            # Thực hiện phân cụm
            print("\nBắt đầu phân cụm học sinh...")
            cluster_labels, k, cluster_centers = cluster_students(students)

            # Tìm điểm đường gần nhất với mỗi tâm cụm ~ bus stop
            print("\nTìm các đường gần nhất cho mỗi cụm...")
            bus_stops_df = find_bus_stops_df(spark, roads_df, cluster_centers)
            bus_stops_df.show()
            spark_write_db("bus_stops", bus_stops_df, "overwrite")

            # Lưu ma trận phân bổ (bus stop - student)
            students_df = spark.createDataFrame([s.student_id for s in students], ["student_id"]) \
                .withColumn("__row_id", monotonically_increasing_id())
            assignments_df = spark.createDataFrame(cluster_labels, ["stop_id"]) \
                .withColumn("stop_id", col("stop_id") + 1) \
                .withColumn("__row_id", monotonically_increasing_id()) \
                .join(students_df, on="__row_id") \
                .drop("__row_id")
            assignments_df.show()
            spark_write_db("assignments", assignments_df, "overwrite")
