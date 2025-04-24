from typing import Callable

from confluent_kafka import Consumer, KafkaException

BOOTSTRAP_SERVERS = "localhost:29092"


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
