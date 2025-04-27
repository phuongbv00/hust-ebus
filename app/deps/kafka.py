import os
import time
from typing import Callable

from confluent_kafka import Consumer, KafkaException
from confluent_kafka.admin import AdminClient

# TODO: BOOTSTRAP_SERVERS = "localhost:29092"
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")


def check_topic_exists(topic_name: str, bootstrap_servers=BOOTSTRAP_SERVERS) -> bool:
    admin = AdminClient({'bootstrap.servers': bootstrap_servers})
    try:
        metadata = admin.list_topics(timeout=5)
        return topic_name in metadata.topics
    except KafkaException as e:
        print(f"[KafkaUtils] Error checking topic existence: {e}")
        return False


def wait_for_topic(
    topic: str,
    retries: int = 100,
    delay: int = 10,
    bootstrap_servers=BOOTSTRAP_SERVERS,
) -> bool:
    for attempt in range(1, retries + 1):
        if check_topic_exists(topic, bootstrap_servers):
            print(f"[KafkaUtils] Kafka topic '{topic}' found.")
            return True
        print(f"[KafkaUtils] Attempt {attempt}/{retries} - Topic '{topic}' not found. Retrying in {delay}s...")
        time.sleep(delay)
    print(f"[KafkaUtils] ERROR - Topic '{topic}' not found after {retries} attempts.")
    return False


def basic_consume_loop(consumer: Consumer, topics: list[str], msg_process: Callable[[str], None]):
    try:
        consumer.subscribe(topics)

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
