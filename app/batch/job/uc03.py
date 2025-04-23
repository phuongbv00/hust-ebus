import math
from batch.core import Job
from deps.spark import get_spark_session, spark_write_db
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, FloatType, IntegerType
from collections import defaultdict
from deps.biz import get_all_bus_stops, get_all_buses, get_all_assignments
from typing import List, Tuple
import pandas as pd

# Bán kính Trái Đất (đơn vị: mét)
EARTH_RADIUS_M = 6_371_000

def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Tính khoảng cách giữa hai điểm trên Trái Đất bằng công thức Haversine.

    Parameters:
    - lat1, lon1: vĩ độ và kinh độ của điểm 1
    - lat2, lon2: vĩ độ và kinh độ của điểm 2

    Returns:
    - distance: khoảng cách giữa hai điểm (mét)
    """
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)

    a = math.sin(delta_phi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    return EARTH_RADIUS_M * c

def assign_stops_with_capacity(
    stops: List[Tuple[str, float, float]],
    buses: List[Tuple[str, float, float, int]],
    assignments: List[Tuple[str, str]]
) -> List[Tuple[str, str, float, int]]:
    """
    Phân bổ xe buýt cho từng điểm dừng sao cho:
    - Khoảng cách là gần nhất
    - Mỗi điểm dừng chỉ gán xe 1 lần
    - Mỗi xe buýt chỉ được sử dụng 1 lần
    - Số lượng học sinh tại điểm dừng không vượt quá sức chứa xe buýt

    Returns:
    - Danh sách (bus_id, stop_id, distance, num_students)
    """

    # Đếm số học sinh đăng ký tại mỗi điểm dừng
    stop_student_count = defaultdict(int)
    for student_id, stop_id in assignments:
        stop_student_count[stop_id] += 1

    # Tính tất cả khoảng cách giữa các bus và stop
    distances = []
    for stop_id, slat, slon in stops:
        for bus_id, blat, blon, _ in buses:
            d = haversine(slat, slon, blat, blon)
            distances.append((d, stop_id, bus_id))

    # Sắp xếp danh sách theo khoảng cách tăng dần
    distances.sort(key=lambda x: x[0])

    # Lưu trữ kết quả phân bổ
    result = []

    # Theo dõi số học sinh đã gán tại mỗi điểm và các bus đã dùng
    assigned_stops = defaultdict(int)
    used_buses = set()
    bus_capacities = {b[0]: b[3] for b in buses}  # bus_id -> capacity

    for d, stop_id, bus_id in distances:
        if assigned_stops[stop_id] >= stop_student_count[stop_id]:
            continue  # Stop đã đủ học sinh

        if bus_id in used_buses:
            continue  # Xe đã được dùng cho stop khác

        remaining_students = stop_student_count[stop_id] - assigned_stops[stop_id]
        bus_capacity = bus_capacities[bus_id]
        assigned = min(remaining_students, bus_capacity)

        result.append((bus_id, stop_id, d, assigned))
        assigned_stops[stop_id] += assigned
        used_buses.add(bus_id)

        # Nếu toàn bộ stop đã được phục vụ đủ thì dừng lại
        if all(assigned_stops[sid] >= stop_student_count[sid] for sid in stop_student_count):
            break

    return result

def get_bus_stop_assignments():
    """
    Lấy dữ liệu từ database, tính toán phân bổ, và ghi kết quả lại vào Spark DataFrame.
    """

    # Lấy dữ liệu từ database
    stops = get_all_bus_stops()  # [(stop_id, lat, lon)]
    buses = get_all_buses()  # [(bus_id, lat, lon, capacity)]
    assignments = get_all_assignments()  # [(student_id, stop_id)]

    # Thực hiện phân bổ bus cho các stop
    records = assign_stops_with_capacity(stops, buses, assignments)

    # Tạo Spark DataFrame để ghi vào database
    spark = get_spark_session()
    df = pd.DataFrame(records, columns=["bus_id", "stop_id", "distance", "num_students"])
    spark_df = spark.createDataFrame(df)

    # Ghi kết quả vào bảng 'bus_assignment'
    spark_write_db('bus_assignment', spark_df, 'overwrite')
    return spark_df

# ---------------- Job class để Spark Scheduler chạy ----------------
class UC03Job(Job):
    def run(self, **kwargs):
        df = get_bus_stop_assignments()
        df.show()  # Hiển thị kết quả trên console
