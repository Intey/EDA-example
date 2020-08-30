from os import environ
from pydantic import BaseModel
import json
from uuid import uuid4
import kafka

REPEATS = int(environ.get("DISTRIBUTION_COUNT", "3"))


class Task(BaseModel):
    id: str
    object: dict
    type: str = "task"


class Routes:
    OBJECTS = "objects"
    TASK_REQUESTS = "tasks-requests"


class MQKafka:
    def __init__(self):
        self.producer = kafka.KafkaProducer(bootstrap_servers="localhost:9092")

    def send(self, obj: dict, route: str):
        f = self.producer.send(route, value=json.dumps(obj).encode("utf8"))
        f.get(timeout=20)

    def accept(self, callback):
        self.consumer = kafka.KafkaConsumer(
            Routes.OBJECTS, enable_auto_commit=False, group_id="distributor"
        )
        for msg in self.consumer:
            callback(json.loads(msg.value.decode("utf-8")))
            self.consumer.commit()


class Distributor:
    """
    Async Distributor based on queue. Accepts message of gotten object,
    and then, repeats it in 3 meta-objects, and distribute them between
    processors
    """

    def __init__(self):
        self.mq = MQKafka()

    def consume(self):
        self.mq.accept(self.process_message)

    def process_message(self, data: dict):
        for i in range(REPEATS):
            task = Task(id=uuid4().hex, object=data)
            print(f"distribute object {data} as {task}")
            self.mq.send(task.dict(), Routes.TASK_REQUESTS)


if __name__ == "__main__":
    distributor = Distributor()
    print("start consume")
    distributor.consume()
