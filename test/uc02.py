import os
import random
from dotenv import load_dotenv
import psycopg

load_dotenv()

DATABASE_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@localhost:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"

def random_offset():
    """Tạo một offset nhỏ trong khoảng ±0.0001 độ (~11m)"""
    return random.uniform(-0.0001, 0.0001)

def update_students_location_with_jitter():
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            # 1. Lấy danh sách student_id, latitude, longitude từ assignment + bus_stops
            cur.execute("""
                SELECT assignments.student_id, bus_stops.latitude, bus_stops.longitude
                FROM assignments
                JOIN bus_stops ON assignments.stop_id = bus_stops.stop_id
            """)
            students = cur.fetchall()

            print(f"Found {len(students)} students to update.")

            # 2. Update từng student
            for student_id, latitude, longitude in students:
                lat_offset = latitude + random_offset()
                lon_offset = longitude + random_offset()

                cur.execute("""
                    UPDATE students
                    SET latitude = %s, longitude = %s
                    WHERE student_id = %s
                """, (lat_offset, lon_offset, student_id))
                print(f"Updated student_id {student_id} to lat={lat_offset}, lon={lon_offset}")

        # Commit toàn bộ thay đổi
        conn.commit()
        print("All updates committed successfully.")

if __name__ == '__main__':
    update_students_location_with_jitter()
