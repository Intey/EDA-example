import kafka
import json
import time
from uuid import uuid4
from tqdm import tqdm
from random import randint
from pydantic import BaseModel


class Routes:
    TASK_REQUESTS = "tasks-requests"
    PROCESSING = "processing"


class MessageTypes:
    start = "START_PROCESSING"
    finish = "END_PROCESSING"


class Message(BaseModel):
    type: str
    object_id: str
    spend_time: int = 0


class MQKafka:
    def __init__(self, consume_route: str, group_id: str = None):
        self.group_id = group_id
        self.consume_route = consume_route
        self.producer = kafka.KafkaProducer(
            bootstrap_servers="localhost:9092",
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

    def send(self, obj: dict, route: str):
        f = self.producer.send(route, value=obj)
        f.get(timeout=20)

    def accept(self, callback):
        self.consumer = kafka.KafkaConsumer(
            self.consume_route,
            group_id=self.group_id,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        )
        for msg in self.consumer:
            callback(msg.value)


class Processor:
    def __init__(self, id_: str):
        self.id = id_
        self.connector = MQKafka(Routes.TASK_REQUESTS)

    def consume(self):
        self.connector.accept(self.callback)

    def callback(self, data: dict):
        data = data.pop("task")
        print(f"processor {self.id} got message '{data}'")
        spend_time = randint(3, 10) * 10
        msg = Message(type=MessageTypes.start, object_id=data["id"])
        self.connector.send(msg.dict(), Routes.PROCESSING)
        for i in tqdm(range(spend_time)):
            time.sleep(0.1)
        print(f"processor {self.id} finish in {spend_time}")
        msg = Message(
            type=MessageTypes.finish, object_id=data["id"], spend_time=spend_time,
        )
        self.connector.send(msg.dict(), Routes.PROCESSING)


if __name__ == "__main__":
    id_ = uuid4().hex
    p = Processor(id_)
    print("start consume")
    p.consume()
