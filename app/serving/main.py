import os

import psycopg
import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from deps.biz import get_hanoi_roads_geojson, DATABASE_URL

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

@app.get("/assignments")
def get_assignments():
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT a.stop_id, s.student_id, s.name, s.latitude, s.longitude
                FROM assignments a
                JOIN students s ON a.student_id = s.student_id
            """)
            assignments = cur.fetchall()
            return [
                {
                    "stop_id": a[0],
                    "student_id": a[1],
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


@app.get("/roads/hanoi")
def get_roads():
    return get_hanoi_roads_geojson("full")


# Entry point for local development
if __name__ == "__main__":
    uvicorn.run("main:app", host="127.0.0.1", port=int(os.getenv("SERVING_PORT", 8000)), reload=True)
