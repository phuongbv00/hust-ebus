import json
import os
from typing import List, Tuple

import psycopg
from dotenv import load_dotenv

from deps.models import Student

load_dotenv()

DATABASE_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@localhost:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"


def get_hanoi_roads_geojson(mode="cropped"):
    """
    :param mode: "cropped" or "full"
    :return:
    """
    with open(f"data/hanoi_roads_{mode}.geojson", "r") as f:
        return json.load(f)


def get_rand_students(limit=100) -> list[Student]:
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                   SELECT student_id, longitude, latitude, name, address
                   FROM students
                   ORDER BY RANDOM()
                   LIMIT {limit}
               """)
            students = cur.fetchall()
            return [
                Student(s[0], s[1], s[2], s[3], s[4])
                for s in students
            ]


def get_students(limit: int | None) -> list[Student]:
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                   SELECT student_id, longitude, latitude, name, address
                   FROM students
                   {f"LIMIT {limit}" if limit else ""}
               """)
            students = cur.fetchall()
            return [
                Student(s[0], s[1], s[2], s[3], s[4])
                for s in students
            ]


def get_all_bus_stops()-> List[Tuple[str, float, float]]:
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT stop_id, latitude, longitude FROM bus_stops")
            return cur.fetchall()


def get_all_buses() -> List[Tuple[str, float, float]]:
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT bus_id, latitude, longitude, capacity FROM buses")
            return cur.fetchall()


def get_all_assignments():
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                    SELECT student_id, stop_id
                    FROM assignments
                """)
            rows = cur.fetchall()
    return rows
