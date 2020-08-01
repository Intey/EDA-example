import pika
from os import environ
import json
import time
from uuid import uuid4
from dataclasses import dataclass


class Routes:
    OBJECTS = "objects"
    TASK_REQUESTS = "tasks-requests"
    PROCESSING = "processing"


class MessageTypes:
    start = "START_PROCESSING"
    finish = "END_PROCESSING"


EXCHANGE = "ebs"


@dataclass
class Store:
    processed: int = 0
    distributed: int = 0
    generated: int = 0
    in_progress: int = 0

    def finish_process_one(self):
        self.processed += 1
        self.in_progress -= 1

    def start_process_one(self):
        self.in_progress += 1

    def add_generated(self):
        self.generated += 1

    def add_distributed(self):
        self.distributed += 1

    def __str__(self):
        str_ = "=" * 80 + "\n"
        str_ += f"{self.processed=}\n"
        str_ += f"{self.distributed=}\n"
        str_ += f"{self.generated=}\n"
        str_ += f"{self.in_progress=}\n"
        str_ += "=" * 80
        return str_


class Staistics:
    def __init__(self, id_: str, stats_db: Store):
        host, port = environ.get("QUEUE_URL").split(":")
        self.conn = pika.BlockingConnection(pika.ConnectionParameters(host, port))
        self.channel = self.conn.channel()
        self.id = id_
        self.stats_db = stats_db

    def consume(self):
        self.channel.exchange_declare(exchange=EXCHANGE, exchange_type="topic")
        result = self.channel.queue_declare("statistics", exclusive=True)
        queue_name = result.method.queue
        self.channel.queue_bind(exchange=EXCHANGE, queue=queue_name, routing_key="*")

        self.channel.basic_consume(
            queue=queue_name, on_message_callback=self.callback, auto_ack=True
        )

        self.channel.start_consuming()

    def callback(self, ch, method, properties, body):
        data = json.loads(body.decode("utf8"))
        if method.routing_key == Routes.PROCESSING:
            if data.get("type") == MessageTypes.finish:
                self.stats_db.finish_process_one()
            elif data.get("type") == MessageTypes.start:
                self.stats_db.start_process_one()
            else:
                print(f"wtf message: {data=}")
            # dispatch event, that
        if method.routing_key == Routes.OBJECTS:
            self.stats_db.add_generated()
        if method.routing_key == Routes.TASK_REQUESTS:
            self.stats_db.add_distributed()

        print(self.stats_db)


if __name__ == "__main__":
    id_ = uuid4().hex
    stats_db = Store()
    p = Staistics(id_, stats_db)
    p.consume()
