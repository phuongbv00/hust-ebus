import json

import pandas as pd
import psycopg
from confluent_kafka import Consumer, KafkaException, OFFSET_END

from deps.biz import DATABASE_URL
from deps.utils import haversine_distance


def msg_process(msg_value):
    payload = json.loads(msg_value).get("payload")
    if not payload or not payload.get("after"):
        return

    student = payload["after"]
    lat_s = student["latitude"]
    lon_s = student["longitude"]
    student_id = student["student_id"]

    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            # Query all bus stops
            cur.execute("SELECT stop_id, latitude, longitude FROM bus_stops")
            stops = cur.fetchall()

            df_stops = pd.DataFrame(stops, columns=["stop_id", "latitude", "longitude"])
            # Compute haversine distances to all stops
            df_stops["distance"] = df_stops.apply(
                lambda row: haversine_distance(lat_s, lon_s, row["latitude"], row["longitude"]),
                axis=1
            )

            # Find the stop with the minimum distance
            nearest_row = df_stops.loc[df_stops["distance"].idxmin()]
            nearest_stop = nearest_row["stop_id"]
            min_distance = nearest_row["distance"]

            # Update assignments table
            cur.execute("""
                INSERT INTO assignments (stop_id, student_id)
                VALUES (%s, %s)
                ON CONFLICT (student_id) DO UPDATE SET stop_id = EXCLUDED.stop_id
            """, (nearest_stop, student_id))
            conn.commit()
            print(f"UC02: Assigned student {student_id} to stop {nearest_stop} (distance: {min_distance:.2f} km)")


def basic_consume_loop(consumer: Consumer, topics: list[str]):
    def my_assign(consumer, partitions):
        for p in partitions:
            p.offset = OFFSET_END
        consumer.assign(partitions)

    try:
        consumer.subscribe(topics, on_assign=my_assign)

        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None: continue

            if msg.error():
                raise KafkaException(msg.error())
            else:
                msg_str = msg.value().decode("utf-8")
                # print(f"Received message: {msg_str}")
                msg_process(msg_str)
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()


def run():
    consumer_conf = {
        'bootstrap.servers': 'localhost:29092',
        'group.id': 'pipeline-uc02-group',
        'auto.offset.reset': 'latest'
    }
    consumer = Consumer(consumer_conf)
    topics = ['pgserver.public.students']
    basic_consume_loop(consumer, topics)
