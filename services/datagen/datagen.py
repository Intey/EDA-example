from fastapi import FastAPI
from pydantic import BaseModel
import mimesis
import json

from kafka import KafkaProducer

gen = mimesis.Generic()


class Object(BaseModel):
    id: int
    name: str
    content: str
    type: str = "object"


class Message(BaseModel):
    type: str = "GENERATE_OBJECT"
    object: Object


class MyKafkaProducer:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers="localhost:9092",
            value_serializer=lambda x: json.dumps(x).encode("utf8"),
        )

    def send(self, obj: Object):
        f = self.producer.send("objects", value=Message(object=obj).dict())
        # sync send
        f.get(timeout=60)


app = FastAPI()


@app.get("/objects/{object_id}")
def get_object(object_id: int) -> Object:
    return Object(id=object_id, name="dump", content="lorem?")


@app.post("/objects")
def create():
    """
    Generate new object. expects that called from Cron Job
    """
    obj = generate()
    prod = MyKafkaProducer()
    prod.send(obj)


def generate() -> Object:
    return Object(
        id=gen.numbers.integer_number(start=0),
        name=gen.text.word(),
        content=gen.text.word(),
    )
