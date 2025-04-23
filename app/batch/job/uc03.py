import math
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, radians, sin, cos, asin, sqrt, pow,
    row_number, lit, coalesce
)
from pyspark.sql.window import Window
from batch.core import Job
from deps.spark import get_spark_session, spark_read_db, spark_write_db
from pyspark.sql.functions import sum as _sum
from pyspark.sql.types import StructType, StructField, StringType, DoubleType


# ---------------- Hàm chung: greedy assignment dataframe ----------------
# def greedy_bus_assignment(
#         buses_df: DataFrame,
#         stops_df: DataFrame,
#         counts_df: DataFrame
# ) -> DataFrame:
#     """
#     Thuật toán greedy 2 vòng trên Spark DataFrame:
#     - Vòng 1: Mỗi bus gán với stop gần nhất.
#     - Vòng 2: Kiểm tra số lượng học sinh, nếu quá tải, phân bổ thêm xe bus gần nhất.
#
#     args:
#     - buses_df: DataFrame ['bus_id', 'bus_lat', 'bus_lon', 'capacity']
#     - stops_df: DataFrame ['stop_id', 'stop_lat', 'stop_lon']
#     - counts_df: DataFrame ['stop_id', 'student_count']
#
#     return: DataFrame ['bus_id', 'stop_id', 'distance']
#     """
#     # Alias cho các DataFrame
#     buses_df_alias = buses_df.alias("buses")
#     stops_df_alias = stops_df.alias("stops")
#     counts_df_alias = counts_df.alias("counts")
#
#     # Bước chuẩn bị trạng thái stops: thêm cột student_count
#     stops_state = stops_df_alias.join(counts_df_alias, 'stop_id', 'left') \
#         .fillna(0, subset=['student_count'])
#
#     # Hàm phụ: tính rank bus-stop theo distance
#     def compute_ranked(df_buses: DataFrame, df_stops: DataFrame) -> DataFrame:
#         # Cross-join để tạo mọi cặp bus-stop
#         joined = df_buses.crossJoin(df_stops)
#
#         # Tính khoảng cách Haversine
#         lat1 = radians(col('buses.bus_lat'))
#         lon1 = radians(col('buses.bus_lon'))
#         lat2 = radians(col('stops.stop_lat'))
#         lon2 = radians(col('stops.stop_lon'))
#
#         dlat = lat2 - lat1
#         dlon = lon2 - lon1
#         expr_a = pow(sin(dlat / 2), 2) + cos(lat1) * cos(lat2) * pow(sin(dlon / 2), 2)
#         expr_c = 2 * asin(sqrt(expr_a))
#
#         with_dist = joined.withColumn('distance', lit(6371000) * expr_c)
#
#         # Xếp hạng mỗi bus theo distance (từ nhỏ đến lớn)
#         window = Window.partitionBy('buses.bus_id').orderBy(col('distance'))
#         return with_dist.withColumn('rank', row_number().over(window))
#
#     # Vòng 1: mỗi bus chọn stop gần nhất
#     ranked_df = compute_ranked(buses_df_alias, stops_state)
#
#     # Xử lý trường hợp có nhiều xe bus có khoảng cách bằng nhau
#     bus_stop_assignment = ranked_df.filter(col('rank') == 1) \
#         .select('buses.bus_id', 'stops.stop_id', 'distance')
#
#     # Kiểm tra xem có điểm dừng nào chưa có xe bus
#     assigned_stops = bus_stop_assignment.select('stops.stop_id').distinct()
#     all_stops = stops_state.select('stop_id').distinct()
#
#     # Tìm các điểm dừng chưa được gán
#     unassigned_stops = all_stops.subtract(assigned_stops)
#
#     # Đảm bảo các điểm dừng chưa được gán sẽ được xe bus phân bổ
#     if unassigned_stops.count() > 0:
#         # Lấy tất cả xe bus chưa được gán
#         unassigned_buses = buses_df_alias.join(bus_stop_assignment.alias("assignments"),
#                                                buses_df_alias['bus_id'] != bus_stop_assignment['bus_id'],
#                                                'leftanti')
#         # Lặp qua các điểm dừng chưa được gán và gán xe bus gần nhất
#         for stop in unassigned_stops.collect():
#             stop_id = stop['stop_id']
#             # Tính khoảng cách giữa các xe bus và điểm dừng chưa được gán
#             temp_ranked = compute_ranked(unassigned_buses, stops_state.filter(col('stop_id') == stop_id))
#             # Gán xe bus gần nhất cho điểm dừng
#             closest_bus = temp_ranked.filter(col('rank') == 1).select('buses.bus_id', 'stops.stop_id', 'distance')
#             bus_stop_assignment = bus_stop_assignment.union(closest_bus)
#
#     # Kiểm tra số lượng học sinh tại các điểm dừng và phân bổ xe bus nếu cần
#     # Gộp thông tin số lượng học sinh tại điểm dừng với bus assignment
#     bus_stop_with_count = bus_stop_assignment.join(counts_df_alias, 'stop_id', 'left')
#
#     # Phân bổ các xe bus nếu điểm dừng quá tải
#     def reassign_buses(bus_stop_with_count: DataFrame, buses_df: DataFrame):
#         overloaded_stops = bus_stop_with_count.filter(
#             col('counts.student_count') > buses_df.filter(col('buses.bus_id') == bus_stop_with_count['buses.bus_id'])[
#                 'buses.capacity']
#         )
#
#         # Tìm xe bus chưa được gán và gán cho điểm dừng quá tải
#         unassigned_buses = buses_df_alias.join(bus_stop_assignment.alias("assignments"),
#                                                buses_df_alias['bus_id'] != bus_stop_assignment['bus_id'],
#                                                'leftanti')
#
#         unassigned_buses_ranked = compute_ranked(unassigned_buses, overloaded_stops)
#
#         # Chọn xe bus gần nhất cho điểm dừng quá tải
#         reassignments = unassigned_buses_ranked.filter(col('rank') == 1) \
#             .select('buses.bus_id', 'stops.stop_id', 'distance')
#         return reassignments
#
#     # Tạo bảng gán lại các xe bus cho điểm dừng nếu có
#     reassignments = reassign_buses(bus_stop_with_count, buses_df_alias)
#
#     # Gộp kết quả gán ban đầu và kết quả gán lại
#     final_assignment = bus_stop_assignment.union(reassignments).distinct()
#
#     return final_assignment


def greedy_bus_assignment(
        buses_df: DataFrame,
        stops_df: DataFrame,
        counts_df: DataFrame
) -> DataFrame:
    """
    Thuật toán greedy 2 vòng trên Spark DataFrame:
    - Vòng 1: Mỗi stop gán với bus gần nhất (ưu tiên khoảng cách, sau đó tới rank).
    - Vòng 2: Kiểm tra số lượng học sinh, nếu quá tải, phân bổ thêm xe bus gần nhất.

    args:
    - buses_df: DataFrame ['bus_id', 'bus_lat', 'bus_lon', 'capacity']
    - stops_df: DataFrame ['stop_id', 'stop_lat', 'stop_lon']
    - counts_df: DataFrame ['stop_id', 'student_count']

    return: DataFrame ['bus_id', 'stop_id', 'distance']
    """

    spark = get_spark_session()

    # Bước chuẩn bị: thêm student_count vào stops
    stops_state = stops_df.join(counts_df, 'stop_id', 'left') \
        .fillna(0, subset=['student_count'])

    # ---------------- Hàm tính khoảng cách ----------------
    def compute_distance(df_buses: DataFrame, df_stops: DataFrame) -> DataFrame:
        joined = df_buses.crossJoin(df_stops)

        lat1 = radians(col('bus_lat'))
        lon1 = radians(col('bus_lon'))
        lat2 = radians(col('stop_lat'))
        lon2 = radians(col('stop_lon'))

        dlat = lat2 - lat1
        dlon = lon2 - lon1
        expr_a = pow(sin(dlat / 2), 2) + cos(lat1) * cos(lat2) * pow(sin(dlon / 2), 2)
        expr_c = 2 * asin(sqrt(expr_a))

        with_dist = joined.withColumn('distance', lit(6371000) * expr_c)
        return with_dist.select('bus_id', 'stop_id', 'distance')

    # ---------------- Vòng 1: Gán mỗi stop cho bus gần nhất ----------------
    distance_df = compute_distance(buses_df, stops_state)

    # Thêm chỉ số thứ tự để ưu tiên theo khoảng cách tăng dần
    window_all = Window.orderBy("distance")
    ranked_df = distance_df.withColumn("rank", row_number().over(window_all))

    # Gán stop-bus duy nhất dựa trên khoảng cách tăng dần mà không dùng collect/count/rdd
    ranked_df = ranked_df.withColumn("bus_stop", col("bus_id") + lit("_") + col("stop_id"))
    assigned = ranked_df.alias("a")

    for_join = ranked_df.select("bus_id", "stop_id").alias("b")

    # Tìm assignment đầu tiên của mỗi bus_id và stop_id
    window_bus = Window.partitionBy("bus_id").orderBy("distance")
    window_stop = Window.partitionBy("stop_id").orderBy("distance")

    bus_first = ranked_df.withColumn("rn_bus", row_number().over(window_bus)).filter(col("rn_bus") == 1)
    bus_first.show()
    stop_first = ranked_df.withColumn("rn_stop", row_number().over(window_stop)).filter(col("rn_stop") == 1)
    stop_first.show()

    # Join lại để lấy giao của cả hai điều kiện
    first_assignment = bus_first.join(stop_first, ["bus_id", "stop_id", "distance"], "inner").select("bus_id",
                                                                                                     "stop_id",
                                                                                                     "distance")
    first_assignment.show()

    # ---------------- Kiểm tra quá tải ----------------
    bus_stop_with_count = first_assignment.join(counts_df, 'stop_id', 'left')
    buses_with_capacity = buses_df.select('bus_id', 'capacity')

    bus_stop_with_capacity = bus_stop_with_count.join(
        buses_with_capacity, on='bus_id'
    )

    overloaded = bus_stop_with_capacity.filter(
        col('student_count') > col('capacity')
    )

    overloaded_stops = overloaded.select('stop_id').distinct()

    unassigned_buses = buses_df.join(
        first_assignment.select('bus_id').distinct(),
        on='bus_id',
        how='left_anti'
    )

    distance_extra = compute_distance(unassigned_buses, stops_state.join(overloaded_stops, on='stop_id'))

    window_extra = Window.orderBy("distance")
    extra_ranked = distance_extra.withColumn("rank", row_number().over(window_extra))

    window_bus_extra = Window.partitionBy("bus_id").orderBy("distance")
    window_stop_extra = Window.partitionBy("stop_id").orderBy("distance")

    extra_bus_first = extra_ranked.withColumn("rn_bus", row_number().over(window_bus_extra)).filter(col("rn_bus") == 1)
    extra_stop_first = extra_ranked.withColumn("rn_stop", row_number().over(window_stop_extra)).filter(
        col("rn_stop") == 1)

    extra_assignment = extra_bus_first.join(extra_stop_first, ["bus_id", "stop_id", "distance"], "inner").select(
        "bus_id", "stop_id", "distance")

    final_assignment = first_assignment.unionByName(extra_assignment)

    return final_assignment


# ---------------- Class UC03Job ----------------
class UC03Job(Job):
    def run(self, **kwargs):
        """
        Khởi tạo và gọi hàm greedy_bus_assignment, sau đó ghi kết quả.
        """
        spark = get_spark_session()

        # Đọc các bảng cần thiết
        buses_df = spark_read_db(
            'SELECT bus_id, latitude AS bus_lat, longitude AS bus_lon, capacity FROM buses'
        )  # DataFrame xe
        stops_df = spark_read_db(
            'SELECT stop_id, latitude AS stop_lat, longitude AS stop_lon FROM bus_stops'
        )  # DataFrame điểm dừng
        counts_df = spark_read_db(
            'SELECT stop_id, COUNT(student_id) AS student_count FROM assignments GROUP BY stop_id'
        )

        # Gọi function chung để phân bổ
        result_df = greedy_bus_assignment(buses_df, stops_df, counts_df)

        spark_write_db('bus_assign_by_rank_df',result_df, 'overwrite')

        # Lưu kết quả bus_id - stop_id - distance - total_students
        # spark_write_db('bus_assignment', result_df.select('bus_id','stop_id','distance','total_students'), 'overwrite')
