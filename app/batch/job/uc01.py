import json
from datetime import datetime
from typing import List, Dict, Tuple

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
from scipy.spatial.distance import cdist, euclidean
from sklearn.cluster import DBSCAN, KMeans
from sklearn.preprocessing import StandardScaler

from batch.core import Job
from deps.models import Student, Point, Road


def get_students() -> np.ndarray:
    """
    Trả về danh sách học sinh cố định
    """
    students = np.array([
        Student(1, "Dương Nguyễn", "Cầu Giấy", 105.8171361, 21.0437571),
        Student(2, "Hạnh Hữu Bùi", "Đống Đa", 105.8138053, 21.0329266),
        Student(3, "Phương Phạm", "Cầu Giấy", 105.8241843, 20.8144491),
        Student(4, "Hoàng Mai", "Tây Hồ", 105.8380745, 20.8737133),
        Student(5, "Nhật Trần", "Cầu Giấy", 105.8381298, 20.8734995),
        Student(6, "Nhật Dương", "Hoàng Mai", 105.5303894, 21.0094937),
        Student(7, "Lâm Lê", "Nhật Tân", 105.5501389, 21.0075942),
        Student(8, "Yến Phạm", "Cầu Giấy", 105.8374884, 20.8760829),
        Student(9, "Tú Nguyễn", "Nam Trung Yên", 105.8105654, 20.9583406),
        Student(10, "Khoa Đặng", "Xuân Hòa", 105.7912191, 21.0133498),
        Student(11, "Nhật Vũ", "Cầu Giấy", 105.8254545, 20.9761357),
        Student(12, "Lâm Mai", "Trung Kính", 105.5438112, 20.8698384),
        Student(13, "Xuân Hữu", "Chính Kinh", 105.5437357, 20.8697357),
        Student(14, "Trần Phương", "Nhân Hòa", 105.8479656, 21.0234488),
        Student(15, "Dương Phạm", "Trung Kính", 105.848663, 21.0241935),
        Student(16, "Đặng Vũ", "Cầu Giấy", 105.8487739, 21.0266572),
        Student(17, "Nguyễn Yến", "Cầu Giấy", 105.8490887, 21.0290919),
        Student(18, "Xuân Dương", "Cầu Giấy", 105.8604224, 21.0008398),
        Student(19, "Yến Trần", "Cầu Giấy", 105.8485786, 21.0132526),
        Student(20, "Xuân Mai", "Cầu Giấy", 105.8534511, 21.0356288),
    ])
    return students


def prepare_student_data(students: np.ndarray) -> np.ndarray:
    """Chuẩn bị dữ liệu học sinh cho clustering"""
    # Lấy tọa độ từ danh sách học sinh
    X = np.array([[student.latitude, student.longitude] for student in students])

    # Chuẩn hóa dữ liệu
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    return X_scaled


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


def kmeans_clustering(X: np.ndarray, n_clusters: int) -> Tuple[np.ndarray, KMeans]:
    """Thực hiện phân cụm K-means"""
    kmeans = KMeans(n_clusters=n_clusters, random_state=42)
    cluster_labels = kmeans.fit_predict(X)

    return cluster_labels, kmeans


def visualize_clusters(X: np.ndarray, labels: np.ndarray, title: str):
    """Vẽ biểu đồ phân cụm"""
    plt.figure(figsize=(10, 6))
    scatter = plt.scatter(X[:, 0], X[:, 1], c=labels, cmap='viridis')
    plt.colorbar(scatter)
    plt.title(title)
    plt.xlabel('Latitude (scaled)')
    plt.ylabel('Longitude (scaled)')
    plt.savefig(f'{title.lower().replace(" ", "_")}.png')
    plt.close()


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


def cluster_students(students: np.ndarray) -> Tuple[np.ndarray, int, KMeans]:
    """Hàm chính để phân cụm học sinh"""
    # Chuẩn bị dữ liệu
    X = prepare_student_data(students)

    # Xác định K bằng DBSCAN
    k = determine_k_with_dbscan(X)
    print(f"Số cụm được xác định bởi DBSCAN: {k}")

    # Thực hiện K-means
    kmeans = KMeans(n_clusters=k, random_state=42)
    cluster_labels = kmeans.fit_predict(X)

    # In thông tin về các cụm
    print("\nThông tin chi tiết về các cụm:")
    print_cluster_coordinates(kmeans.cluster_centers_, cluster_labels, students)

    return cluster_labels, k, kmeans


def read_points_data(file_path: str) -> List[Point]:
    """
    Đọc dữ liệu điểm từ file GeoJSON

    Args:
        file_path: Đường dẫn đến file GeoJSON

    Returns:
        List[Point]: Danh sách các đối tượng Point
    """
    try:
        # Đọc file GeoJSON bằng geopandas
        gdf = gpd.read_file(file_path)

        points = []
        for _, row in gdf.iterrows():
            feature = {
                'geometry': row.geometry.__geo_interface__,
                'properties': {
                    'osm_id': row.get('osm_id', ''),
                    'name': row.get('name', ''),
                    'addr:street': row.get('addr:street', ''),
                    'amenity': row.get('amenity', '')
                }
            }
            try:
                point = Point.from_geojson_feature(feature)
                points.append(point)
            except Exception as e:
                # Skip points that can't be processed
                continue

        print(f"Đã đọc thành công {len(points)} điểm từ file")
        return points

    except Exception as e:
        print(f"Lỗi khi đọc file points: {str(e)}")
        return []


def find_nearest_roads_for_clusters(roads: List[Road],
                                    cluster_centers: np.ndarray,
                                    cluster_labels: np.ndarray,
                                    students: np.ndarray,
                                    max_roads: int = 3) -> Dict[int, List[Tuple[Road, float]]]:
    """Tìm các đường gần nhất với mỗi cụm học sinh"""
    nearest_roads = {}
    road_coordinates = []

    for road in roads:
        if road.coordinates:
            road_center = np.mean(road.coordinates, axis=0)
            road_coordinates.append(road_center)
    road_coordinates = np.array(road_coordinates)

    for cluster_id in range(len(cluster_centers)):
        cluster_students = students[cluster_labels == cluster_id]
        cluster_center = np.mean([[s.latitude, s.longitude] for s in cluster_students], axis=0)
        distances = cdist([cluster_center], road_coordinates)[0]
        nearest_indices = np.argsort(distances)[:max_roads]

        cluster_roads = []
        for idx in nearest_indices:
            road = roads[idx]
            distance = distances[idx]
            cluster_roads.append((road, distance))

        nearest_roads[cluster_id] = cluster_roads

    return nearest_roads


def find_nearest_points_for_clusters(roads: List[Road],
                                     cluster_centers: np.ndarray,
                                     cluster_labels: np.ndarray,
                                     students: np.ndarray) -> List[Tuple[Road, float]]:
    """
    Tìm đường gần nhất với mỗi cụm học sinh

    Args:
        roads: Danh sách các đường từ GeoJSON
        cluster_centers: Tọa độ tâm các cụm (đã chuẩn hóa)
        cluster_labels: Nhãn cụm của từng học sinh
        students: Danh sách học sinh

    Returns:
        List[Tuple[Road, float]]: List các tuple (đường gần nhất, khoảng cách) cho mỗi cụm
    """
    nearest_roads = []

    # Với mỗi cụm
    for cluster_id in range(len(set(cluster_labels))):
        # Lấy học sinh trong cụm
        cluster_students = students[cluster_labels == cluster_id]

        # Tính trung tâm thực tế của cụm từ vị trí học sinh
        cluster_center = np.mean([[s.latitude, s.longitude] for s in cluster_students], axis=0)

        # Tìm đường gần nhất với trung tâm cụm
        min_distance = float('inf')
        nearest_road = None

        # Tính khoảng cách từ tâm cụm đến từng đường
        for road in roads:
            if road.coordinates:  # Kiểm tra đường có tọa độ không
                # Tính điểm trung tâm của đường
                road_center = np.mean(road.coordinates, axis=0)

                # Tính khoảng cách từ tâm cụm đến trung tâm đường
                distance = euclidean(
                    [cluster_center[0], cluster_center[1]],
                    [road_center[1], road_center[0]]  # Đổi vị trí vì trong GeoJSON là [lon, lat]
                )

                # Cập nhật đường gần nhất nếu tìm thấy khoảng cách ngắn hơn
                if distance < min_distance:
                    min_distance = distance
                    nearest_road = road

        if nearest_road:
            nearest_roads.append((nearest_road, min_distance))
            print(f"\nCụm {cluster_id + 1}:")
            print(f"Tâm cụm: ({cluster_center[0]:.6f}, {cluster_center[1]:.6f})")
            print(f"Đường gần nhất: {nearest_road.name or 'Không có tên'}")
            print(f"Loại đường: {nearest_road.highway}")
            road_center = np.mean(nearest_road.coordinates, axis=0)
            print(f"Tọa độ trung tâm đường: ({road_center[1]:.6f}, {road_center[0]:.6f})")
            print(f"Khoảng cách: {min_distance:.6f} km")

    return nearest_roads


def print_cluster_roads(nearest_roads: List[Tuple[Road, float]]):
    """In thông tin về đường gần nhất của mỗi cụm"""
    for i, (road, distance) in enumerate(nearest_roads):
        print(f"\nCụm {i + 1}:")
        print(f"Đường gần nhất:")
        print(f"- Tên: {road.name or 'Không có tên'}")
        print(f"- Loại đường: {road.highway}")
        if road.coordinates:
            road_center = np.mean(road.coordinates, axis=0)
            print(f"- Tọa độ trung tâm: ({road_center[1]:.6f}, {road_center[0]:.6f})")
        print(f"- Khoảng cách đến tâm cụm: {distance:.6f} km")


def print_cluster_points(nearest_points: List[Tuple[Point, float]]):
    """In thông tin về điểm gần nhất của mỗi cụm"""
    for i, (point, distance) in enumerate(nearest_points):
        print(f"\nCụm {i + 1}:")
        print(f"Điểm gần nhất:")
        print(f"- ID: {point.point_id}")
        print(f"- Tên: {point.name or 'Không có tên'}")
        print(f"- Địa chỉ: {point.address or 'Không có địa chỉ'}")
        print(f"- Loại: {point.amenity or 'Không xác định'}")
        print(f"- Tọa độ: ({point.latitude}, {point.longitude})")
        print(f"- Khoảng cách: {distance:.4f} km")


def export_clustering_results(students: np.ndarray,
                              cluster_labels: np.ndarray,
                              cluster_centers: np.ndarray,
                              nearest_roads: List[Tuple[Road, float]],
                              output_file: str = None) -> dict:
    """
    Xuất kết quả phân cụm ra định dạng JSON

    Args:
        students: Danh sách học sinh
        cluster_labels: Nhãn cụm của từng học sinh
        cluster_centers: Tọa độ tâm các cụm
        nearest_roads: Các đường gần nhất với tâm cụm
        output_file: Tên file để lưu JSON (tùy chọn)
    """
    results = {
        "metadata": {
            "total_students": len(students),
            "total_clusters": len(set(cluster_labels)),
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        },
        "clusters": [],
        "students": [],
        "nearest_roads": []
    }

    # Thêm thông tin về các cụm
    for cluster_id in range(len(set(cluster_labels))):
        # Lấy học sinh trong cụm
        cluster_students = students[cluster_labels == cluster_id]
        # Tính tâm thực tế của cụm
        cluster_center = np.mean([[s.latitude, s.longitude] for s in cluster_students], axis=0)

        cluster_info = {
            "cluster_id": cluster_id + 1,
            "coordinates": {
                "latitude": float(cluster_center[0]),
                "longitude": float(cluster_center[1])
            },
            "student_count": len(cluster_students)
        }
        results["clusters"].append(cluster_info)

    # Thêm thông tin về học sinh
    for student, cluster_label in zip(students, cluster_labels):
        student_info = {
            "student_id": student.student_id,
            "name": student.name,
            "address": student.address,
            "coordinates": {
                "latitude": float(student.latitude),
                "longitude": float(student.longitude)
            },
            "cluster_id": int(cluster_label) + 1
        }
        results["students"].append(student_info)

    # Thêm thông tin về các đường gần nhất
    for i, (road, distance) in enumerate(nearest_roads):
        road_center = np.mean(road.coordinates, axis=0) if road.coordinates else [0, 0]
        road_info = {
            "cluster_id": i + 1,
            "name": road.name or "Không có tên",
            "highway": road.highway,
            "coordinates": {
                "latitude": float(road_center[1]),
                "longitude": float(road_center[0])
            },
            "distance_to_center": float(distance)
        }
        results["nearest_roads"].append(road_info)

    # Lưu vào file nếu có tên file
    if output_file:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        print(f"\nĐã xuất dữ liệu ra file: {output_file}")

    return results


def read_road_data(file_path: str) -> List[Road]:
    """
    Đọc dữ liệu đường từ file GeoJSON

    Args:
        file_path: Đường dẫn đến file GeoJSON

    Returns:
        List[Road]: Danh sách các đối tượng Road
    """
    try:
        # Đọc file GeoJSON bằng geopandas
        gdf = gpd.read_file(file_path)

        # Chuyển đổi từng feature thành đối tượng Road
        roads = []
        for _, row in gdf.iterrows():
            feature = {
                'geometry': row.geometry.__geo_interface__,
                'properties': {
                    'name': row.get('name', ''),
                    'oneway': row.get('oneway', False),
                    'highway': row.get('highway', ''),
                    'maxspeed': row.get('maxspeed'),
                    'lanes': row.get('lanes'),
                    'bridge': row.get('bridge', False),
                    'tunnel': row.get('tunnel', False)
                }
            }
            road = Road.from_geojson_feature(feature)
            roads.append(road)

        print(f"Đã đọc thành công {len(roads)} đường từ file")
        return roads

    except Exception as e:
        print(f"Lỗi khi đọc file: {str(e)}")
        return []


class UC01Job(Job):
    def run(self, *args, **kwargs):
        # Đọc dữ liệu đường và học sinh
        file_path = "data/hanoi_roads_full.geojson"
        points_path = "data/hanoi_points_full.geojson"

        roads = read_road_data(file_path)
        points = read_points_data(points_path)
        students = get_students()

        # Thực hiện phân cụm
        if students.size > 0 and roads:
            print("\nBắt đầu phân cụm học sinh...")
            cluster_labels, k, kmeans = cluster_students(students)

            # Tìm đường gần nhất cho mỗi cụm
            print("\nTìm các đường gần nhất cho mỗi cụm...")
            nearest_roads = find_nearest_points_for_clusters(
                roads=roads,
                cluster_centers=kmeans.cluster_centers_,
                cluster_labels=cluster_labels,
                students=students
            )

            # TODO
            # Xuất kết quả ra file JSON
            # output_file = "clustering_results.json"
            # export_clustering_results(
            #     students=students,
            #     cluster_labels=cluster_labels,
            #     cluster_centers=kmeans.cluster_centers_,
            #     nearest_roads=nearest_roads,
            #     output_file=output_file
            # )

            # In kết quả
            print("\nCác đường gần nhất cho mỗi cụm:")
            print_cluster_roads(nearest_roads)
