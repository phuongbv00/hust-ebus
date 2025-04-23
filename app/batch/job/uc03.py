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


# ---------------- Hàm chung: greedy assignment dataframe ----------------
def greedy_bus_assignment(
    buses_df: DataFrame,
    stops_df: DataFrame,
    counts_df: DataFrame
) -> DataFrame:
    """
    Thuật toán greedy 2 vòng trên Spark DataFrame:
    - Vòng 1: mỗi bus lấy stop gần nhất.
    - Vòng 2: bus còn lại lấy stop có student_count > assigned_capacity.

    args:
    - buses_df: ['bus_id','bus_lat','bus_lon','capacity']
    - stops_df: ['stop_id','stop_lat','stop_lon']
    - counts_df: ['stop_id','student_count']

    return DataFrame ['bus_id','stop_id','distance']
    """
    # Bước chuẩn bị trạng thái stops: thêm cột student_count
    stops_state = stops_df.join(counts_df, 'stop_id', 'left') \
        .fillna(0, subset=['student_count'])

    # Hàm phụ: tính rank bus-stop theo distance
    def compute_ranked(df_buses: DataFrame, df_stops: DataFrame) -> DataFrame:
        # Cross-join để tạo mọi cặp bus-stop
        joined = df_buses.crossJoin(df_stops)
        # Tính khoảng cách Haversine
        lat1 = radians(col('bus_lat')); lon1 = radians(col('bus_lon'))
        lat2 = radians(col('stop_lat')); lon2 = radians(col('stop_lon'))
        dlat = lat2 - lat1; dlon = lon2 - lon1
        expr_a = pow(sin(dlat/2), 2) + cos(lat1)*cos(lat2)*pow(sin(dlon/2), 2)
        expr_c = 2 * asin(sqrt(expr_a))
        with_dist = joined.withColumn('distance', lit(6371000) * expr_c)
        # Xếp hạng mỗi bus theo distance (từ nhỏ đến lớn)
        window = Window.partitionBy('bus_id').orderBy(col('distance'))
        return with_dist.withColumn('rank', row_number().over(window))

    # Vòng 1: mỗi bus chọn stop gần nhất
    ranked1 = compute_ranked(buses_df, stops_state)
    first_assign = ranked1.filter(col('rank') == 1) \
        .select('bus_id','stop_id','capacity','distance')

    # Tính stops còn thiếu xe bus do số súc vật nhiều hơn số chỗ trên xe: subtract assigned capacity vòng 1
    assigned_cap = first_assign.groupBy('stop_id') \
        .agg({'capacity':'sum'}) \
        .withColumnRenamed('sum(capacity)','assigned_capacity')
    stops_left = stops_state.join(assigned_cap, 'stop_id', 'left') \
        .withColumn('assigned_capacity', coalesce(col('assigned_capacity'), lit(0))) \
        .withColumn('students_left', col('student_count') - col('assigned_capacity')) \
        .filter(col('students_left') > 0) \
        .select('stop_id','stop_lat','stop_lon')

    # Vòng 2: bus chưa gán tiếp tục chọn stop còn thiếu xe, thừa sinh viên
    # xem bus nào gần với stop thiếu xe thì assign
    used = first_assign.select('bus_id')
    rem_buses = buses_df.join(used, 'bus_id', 'left_anti')
    ranked2 = compute_ranked(rem_buses, stops_left)
    second_assign = ranked2.filter(col('rank') == 1) \
        .select('bus_id','stop_id','distance')

    # Kết hợp vòng 1 & vòng 2
    # Kết hợp vòng 1 & vòng 2, thêm distance
    combined = first_assign.select('bus_id','stop_id','distance') \
        .unionByName(second_assign)

    # Tính tổng số sinh viên đã đón theo từng bus
    combined_with_capacity = combined.join(
        buses_df.select('bus_id', 'capacity'), on='bus_id', how='left'
    )

    total_students_df = combined_with_capacity.groupBy('bus_id') \
        .agg(_sum('capacity').alias('total_students'))

    return combined.join(total_students_df, on='bus_id', how='left').orderBy('bus_id')

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

        # Lưu kết quả bus_id - stop_id - distance - total_students
        spark_write_db('bus_assignment', result_df.select('bus_id','stop_id','distance','total_students'), 'overwrite')
