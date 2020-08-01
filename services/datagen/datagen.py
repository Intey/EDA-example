from fastapi import FastAPI
from pydantic import BaseModel
import pika
from os import environ
import mimesis
import json

gen = mimesis.Generic()


class Object(BaseModel):
    id: int
    name: str
    content: str


app = FastAPI()

EXCHANGE = "ebs"


@app.get("/objects/{object_id}")
def get_object(object_id: int) -> Object:
    return Object(id=object_id, name="dump", content="lorem?")


@app.post("/objects")
def create():
    """
    Generate new object. expects that called from Cron Job
    """
    obj = generate()
    global connection
    host, port = environ.get("QUEUE_URL").split(":")
    connection = pika.BlockingConnection(pika.ConnectionParameters(host, port))
    send_to_queue(connection, obj)


def generate() -> Object:
    return Object(
        id=gen.numbers.integer_number(start=0),
        name=gen.text.word(),
        content=gen.text.word(),
    )


def send_to_queue(connection, obj: Object):
    print("send message", obj)
    channel = connection.channel()
    channel.exchange_declare(exchange=EXCHANGE, exchange_type="topic")
    channel.basic_publish(
        exchange=EXCHANGE, routing_key="objects", body=json.dumps(obj.dict())
    )
    connection.close()
