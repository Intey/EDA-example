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


class MyKafkaProducer:
    def __init__(self):
        self.producer = KafkaProducer(bootstrap_servers="localhost:9092")

    def send(self, obj: Object):
        f = self.producer.send("objects", value=json.dumps(obj.dict()).encode("utf8"))
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
