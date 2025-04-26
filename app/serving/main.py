import csv
import io
import os
import random
import psycopg
import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from deps.biz import get_hanoi_roads_geojson, DATABASE_URL
from typing import List, Dict
# Load environment variables from .env
load_dotenv()

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allow all methods (GET, POST, etc.)
    allow_headers=["*"],  # Allow all headers
)

def read_hanoi_points(path: str) -> List[Dict[str, str]]:
    """Đọc file CSV hanoi_points_full và trả về list các dict."""
    points = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            points.append({
                "address": row["name"],
                "longitude": float(row["longitude"]),
                "latitude": float(row["latitude"]),
            })
    return points

@app.get("/assignments")
def get_assignments():
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT a.student_id, a.stop_id, s.name, s.latitude, s.longitude
                FROM assignments a
                JOIN students s ON a.student_id = s.student_id
            """)
            assignments = cur.fetchall()
            return [
                {
                    "student_id": a[0],
                    "stop_id": a[1],
                    "name": a[2],
                    "latitude": a[3],
                    "longitude": a[4]
                }
                for a in assignments
            ]


@app.get("/bus-stops")
def get_bus_stops():
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT stop_id, road_id, latitude, longitude FROM bus_stops")
            bus_stops = cur.fetchall()
            return [
                {
                    "stop_id": b[0],
                    "road_id": b[1],
                    "latitude": b[2],
                    "longitude": b[3],
                }
                for b in bus_stops
            ]

@app.get("/buses")
def get_bus_stops():
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT bus_id, capacity, latitude, longitude FROM buses")
            buses = cur.fetchall()
            return [
                {
                    "bus_id": b[0],
                    "capacity": b[1],
                    "latitude": b[2],
                    "longitude": b[3],
                }
                for b in buses
            ]


@app.get("/roads/hanoi")
def get_roads():
    return get_hanoi_roads_geojson("full")


@app.post("/upload-csv")
async def upload_csv(file: UploadFile = File(...)):
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Chỉ chấp nhận file CSV")

    content = await file.read()
    decoded = content.decode("utf-8")
    csv_reader = csv.reader(io.StringIO(decoded))

    # Bỏ dòng header
    rows = list(csv_reader)[1:]

    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                # Xoá toàn bộ dữ liệu hiện tại
                cur.execute("DELETE FROM students")

                # Ghi dữ liệu mới
                for row in rows:
                    student_id, longitude, latitude, name, address = row
                    cur.execute(
                        """
                        INSERT INTO students (student_id, name, address, latitude, longitude)
                        VALUES (%s, %s, %s, %s, %s)
                        """,
                        (int(student_id), name, address, float(latitude), float(longitude))
                    )
                conn.commit()

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Lỗi ghi dữ liệu: {e}")

    return {"message": f"Tải lên và ghi đè {len(rows)} học sinh thành công"}


@app.get("/student-clusters")
def get_student_clusters():
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT cluster_id, latitude, longitude FROM student_clusters")
            clusters = cur.fetchall()
            return [
                {
                    "cluster_id": b[0],
                    "latitude": b[1],
                    "longitude": b[2],
                }
                for b in clusters
            ]


@app.get("/bus-assignments")
def get_bus_assignments():
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT bus_id,stop_id,distance,num_students FROM bus_assignments")
            bus_assignments = cur.fetchall()
            return [
                {
                    "bus_id": b[0],
                    "stop_id": b[1],
                    "distance": b[2],
                    "num_students": b[3],
                }
                for b in bus_assignments
            ]

@app.post("/reassign-student-locations")
def reassign_student_locations():
    """
    Lấy ngẫu nhiên các điểm trong hanoi_points_full.csv để gán lại
    longitude/latitude cho tất cả sinh viên, và trả về danh sách sinh viên.
    """
    # 1. Đọc toàn bộ điểm
    csv_path = "data/hanoi_points_full.csv"
    if not os.path.exists(csv_path):
        raise HTTPException(status_code=500, detail="Không tìm thấy file hanoi_points_full.csv")

    points = read_hanoi_points(csv_path)
    if not points:
        raise HTTPException(status_code=500, detail="File hanoi_points_full.csv trống")

    try:
        # 1) Lấy danh sách student_id
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT student_id FROM students ORDER BY student_id")
                student_ids = [row[0] for row in cur.fetchall()]

        n = len(student_ids)
        if n == 0:
            return {"message": "Không có sinh viên nào để cập nhật", "students": []}

        # 2) Lấy ngẫu nhiên N điểm (có thể lặp lại nếu points < n)
        chosen = random.choices(points, k=1000)

        # 3) Cập nhật từng sinh viên
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                for sid, pt in zip(student_ids, chosen):
                    cur.execute(
                        """
                        UPDATE students
                        SET address   = %s,
                            longitude = %s,
                            latitude  = %s
                        WHERE student_id = %s
                        """,
                        (pt["address"], pt["longitude"], pt["latitude"], sid)
                    )
                conn.commit()

        # 4) Trả về danh sách sau cập nhật
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                        SELECT student_id, name, address, latitude, longitude
                        FROM students
                        ORDER BY student_id
                    """)
                rows = cur.fetchall()
                students = [
                    {
                        "student_id": r[0],
                        "name": r[1],
                        "address": r[2],
                        "latitude": r[3],
                        "longitude": r[4],
                    }
                    for r in rows
                ]

        return {"students": students}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Lỗi khi cập nhật: {e}")

# Entry point for local development
if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("SERVING_PORT", 8000)), reload=True)
