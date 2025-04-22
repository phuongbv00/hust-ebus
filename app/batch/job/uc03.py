from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, radians, sin, cos, asin, sqrt, pow,
    row_number, lit, ceil
)
from pyspark.sql.window import Window

from batch.core import Job
from deps.spark import get_spark_session, spark_read_db, spark_write_db

# Earth's radius in meters
EARTH_RADIUS_M = 6_371_000

# def haversine_distance(lat1, lng1, lat2, lng2):
#     """
#     Tính khoảng cách Haversine giữa hai tọa độ địa lý (đơn vị: mét)
#     """
#     lat1, lng1, lat2, lng2 = map(np.radians, [lat1, lng1, lat2, lng2])
#     dlat = lat2 - lat1
#     dlng = lng2 - lng1
#     a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlng / 2.0) ** 2
#     c = 2 * np.arcsin(np.sqrt(a))
#     return EARTH_RADIUS_M * c


def assign_buses_to_stops_df(
    buses_df: DataFrame,
    stops_df: DataFrame,
    assignment_counts_df: DataFrame
) -> DataFrame:
    """
    Dùng Spark DataFrame API để phân bổ xe cho mỗi điểm dừng dựa trên khoảng cách và tải trọng riêng của từng xe.
    - buses_df: ['bus_id','capacity','latitude','longitude']
    - stops_df: ['stop_id','latitude','longitude']
    - assignment_counts_df: ['stop_id','student_count']

    Trả về DataFrame ['bus_id','stop_id','distance'] chỉ chọn các xe gần nhất đủ phục vụ sinh viên tại điểm.
    """
    # Alias cho bus và stop
    b = buses_df.select(
        col('bus_id'),
        col('capacity'),
        col('latitude').alias('bus_lat'),
        col('longitude').alias('bus_lon')
    )
    s = stops_df.select(
        col('stop_id'),
        col('latitude').alias('stop_lat'),
        col('longitude').alias('stop_lon')
    )
    a = assignment_counts_df.select(
        col('stop_id'),
        col('student_count')
    )

    # Cross join mọi cặp bus-stop rồi join student_count
    joined = b.crossJoin(s).join(a, on='stop_id', how='left').fillna({'student_count': 0})

    # Tính khoảng cách Haversine
    # Dùng các hàm Spark (sin, cos, radians, v.v.) để tính ngay trong DataFrame, đảm bảo Spark phân tán và tối ưu hóa công việc tính toán.
    lat1 = radians(col('bus_lat'))
    lon1 = radians(col('bus_lon'))
    lat2 = radians(col('stop_lat'))
    lon2 = radians(col('stop_lon'))
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a_expr = pow(sin(dlat / 2), 2) + cos(lat1) * cos(lat2) * pow(sin(dlon / 2), 2)
    c_expr = 2 * asin(sqrt(a_expr))
    with_dist = joined.withColumn('distance', lit(EARTH_RADIUS_M) * c_expr)

    # Tính số bus cần thiết (buses_needed) tại mỗi stop dựa trên capacity của từng bus
    # Ví dụ nếu có 45 sinh viên và mỗi xe chỉ chở được 30 người, thì 45 / 30 = 1.5, ceil(1.5) = 2 nghĩa là ta cần 2 xe để chở hết
    # ceil(student_count / capacity)
    with_need = with_dist.withColumn(
        'buses_needed',
        ceil(col('student_count') / col('capacity'))
    )

    # Đánh thứ tự xe theo khoảng cách trong mỗi stop
    window = Window.partitionBy('stop_id').orderBy(col('distance'))
    ranked = with_need.withColumn('rank', row_number().over(window))

    # Lấy bus có rank <= buses_needed
    result = ranked.filter(col('rank') <= col('buses_needed'))

    return result.select('bus_id', 'stop_id', 'distance')

class UC03Job(Job):
    def run(self, **kwargs):
        """
        Job phân bổ xe bus cho các điểm dừng, sử dụng DataFrame buses có sẵn.
        """
        spark = get_spark_session()

        # Đọc các bảng từ DB
        buses_df = spark_read_db('SELECT bus_id, latitude, longitude, capacity FROM buses')
        bus_stops_df = spark_read_db('SELECT stop_id, latitude, longitude FROM bus_stops')
        assignment_counts_df = spark_read_db(
            'SELECT stop_id, COUNT(student_id) AS student_count FROM assignments GROUP BY stop_id'
        )
        assignments_raw_df = spark_read_db(
            'SELECT student_id, stop_id FROM assignments'
        )

        # Tính phân bổ bus-stop dựa trên capacity và khoảng cách
        assign_df = assign_buses_to_stops_df(
            buses_df,
            bus_stops_df,
            assignment_counts_df
        )

        # tạo dataframe có bus_id, stop_id, student_id, distance (tính theo haversine)
        bus_assignment_df = assign_df.join(
            assignments_raw_df,
            on='stop_id', how='inner'
        ).select(
            col('bus_id'),
            col('stop_id'),
            col('student_id'),
            col('distance')
        )

        bus_assignment_df.show()  # debug

        # Ghi kết quả ra DB
        spark_write_db('bus_assignment', bus_assignment_df, 'overwrite')

