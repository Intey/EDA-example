import pika  # provider
from os import environ
import json  # serializer


class Consts:
    EXCHANGE = "ebs"
    OBJECTS_RK = "objects"
    TASK_REQUESTS_RK = "tasks-requests"
    PROCESSING_RK = "processing"


class BusAdapter:
    def __init__(provider, serializer):
        pass

    def listen_messages(self, filters):
        pass

    def send_message(self):
        pass


class Distributor:
    class Consts:
        OBJECTS_RK = "objects"
        TASK_REQUESTS_RK = "tasks-requests"
        EXCHANGE = "ebs"

    def __init__(self):
        host, port = environ.get("QUEUE_URL").split(":")
        self.conn = pika.BlockingConnection(pika.ConnectionParameters(host, port))
        self.channel = self.conn.channel()

    def consume(self, callback):
        self.channel.exchange_declare(exchange=self.EXCHANGE, exchange_type="topic")

        q_decl = self.channel.queue_declare("distributor_consume")

        self.channel.queue_bind(
            exchange=self.EXCHANGE,
            queue=q_decl.method.queue,
            routing_key=self.Consts.OBJECTS_RK,
        )

        self.channel.basic_consume(
            queue=q_decl.method.queue, on_message_callback=callback, auto_ack=True
        )
        self.channel.start_consuming()


class Processor:
    class MessageTypes:
        start = "START_PROCESSING"
        finish = "END_PROCESSING"

    def consume(self):
        self.channel.exchange_declare(exchange=Consts.EXCHANGE, exchange_type="topic")
        result = self.channel.queue_declare("processor_consume")
        queue_name = result.method.queue
        self.channel.queue_bind(
            exchange=Consts.EXCHANGE, queue=queue_name, routing_key=Consts.TASK_REQUESTS
        )

        self.channel.basic_consume(
            queue=queue_name, on_message_callback=self.callback, auto_ack=True
        )

        self.channel.start_consuming()

    def get_channel(self):
        host, port = environ.get("QUEUE_URL").split(":")
        connection = pika.BlockingConnection(pika.ConnectionParameters(host, port))
        channel = connection.channel()
        channel.exchange_declare(exchange=self.Consts.EXCHANGE, exchange_type="topic")

    def send_to_queue(self, data: dict):
        channel = self.get_channel()
        result = channel.queue_declare("")

        channel.queue_bind(
            exchange=Consts.EXCHANGE,
            queue=result.method.queue,
            routing_key=Consts.PROCESSING,
        )

        channel.basic_publish(
            exchange=Consts.EXCHANGE,
            routing_key=Consts.PROCESSING_RK,
            body=json.dumps(data),
        )


class Chan:
    def __init__(self, host, port):
        self.host = host
        self.port = port

    def __enter__(self):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(self.host, self.port)
        )
        self.channel = self.connection.channel()

    def __exit__(self):
        self.connection.close()


class Datagen:
    def send_to_queue(self, connection, obj):
        with Chan(*environ.get("QUEUE_URL", "localhost:5672").split(":")) as channel:
            channel.exchange_declare(exchange=Consts.EXCHANGE, exchange_type="topic")
            channel.basic_publish(
                exchange=Consts.EXCHANGE, routing_key="objects", body=json.dumps(obj)
            )
