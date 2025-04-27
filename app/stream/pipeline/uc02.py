import json
from datetime import datetime, timezone

import pandas as pd
import psycopg
from confluent_kafka import Consumer

from deps.biz import DATABASE_URL
from deps.kafka import BOOTSTRAP_SERVERS, basic_consume_loop, wait_for_topic
from deps.utils import haversine_distance


def msg_process(msg_value):
    payload = json.loads(msg_value).get("payload")
    if not payload or not payload.get("after"):
        return

    ts_ms = payload["source"]["ts_ms"]
    timestamp = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
    student = payload["after"]
    lat_s = student["latitude"]
    lon_s = student["longitude"]
    student_id = student["student_id"]

    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            # Insert student_locations
            cur.execute("""
                INSERT INTO student_locations (student_id, latitude, longitude, timestamp)
                VALUES (%s, %s, %s, %s)
            """, (student_id, lat_s, lon_s, timestamp))
            conn.commit()
            print(f"UC02: Updated student {student_id} (location: {lat_s, lon_s}) at {timestamp}")

    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            # Query all bus stops
            cur.execute("SELECT stop_id, latitude, longitude FROM bus_stops")
            stops = cur.fetchall()

    if stops and len(stops) > 0:
        df_stops = pd.DataFrame(stops, columns=["stop_id", "latitude", "longitude"])
        # Compute haversine distances to all stops
        df_stops["distance"] = df_stops.apply(
            lambda row: haversine_distance(lat_s, lon_s, row["latitude"], row["longitude"]),
            axis=1
        )

        # Find the stop with the minimum distance
        nearest_row = df_stops.loc[df_stops["distance"].idxmin()]
        nearest_stop = int(nearest_row["stop_id"])
        min_distance = nearest_row["distance"]

        # Update assignments table
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO assignments (stop_id, student_id)
                    VALUES (%s, %s)
                    ON CONFLICT (student_id) DO UPDATE SET stop_id = EXCLUDED.stop_id
                """, (nearest_stop, student_id))
                conn.commit()
                print(f"UC02: Assigned student {student_id} to stop {nearest_stop} (distance: {min_distance:.2f}m)")


def run():
    print("UC02: Running...")
    topic = "pgserver.public.students"
    if not wait_for_topic(topic):
        return
    consumer_conf = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'group.id': 'pipeline-uc02-group',
        'auto.offset.reset': 'latest'
    }
    consumer = Consumer(consumer_conf)
    basic_consume_loop(consumer, [topic], msg_process)
