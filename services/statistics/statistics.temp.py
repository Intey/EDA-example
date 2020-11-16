import pika
from os import environ
import json
import time
from uuid import uuid4
from dataclasses import dataclass, field, asdict
from aiohttp import web
import threading
from aiohttp_sse import sse_response
import aio_pika
import asyncio

MAIN_HTML = """
<html>
<body>
{{body}}
</body>
<script>
let eventSource = new EventSource("/updates");

eventSource.onmessage = function(event) {
  let data = JSON.parse(event.data)
  console.log(data)
}
</script>
</html>
"""


async def rmq(loop):
    connection = await aio_pika.connect_robust("amqp://localhost:5672/", loop=loop)

    async with connection:
        queue_name = "test_queue"

        # Creating channel
        channel = await connection.channel()  # type: aio_pika.Channel

        # Declaring queue
        queue = await channel.declare_queue(
            queue_name, auto_delete=True
        )  # type: aio_pika.Queue

        async with queue.iterator() as queue_iter:
            # Cancel consuming after __aexit__
            async for message in queue_iter:
                async with message.process():
                    print(message.body)

                    if queue.name in message.body.decode():
                        break


async def main_page(request):
    stats = request.app["store"]
    MAIN_HTML.replace("{{body}}", str(stats))
    return web.Response(text=MAIN_HTML, content_type="text/html")


async def update_subscription(request):
    loop = request.app.loop
    store = request.app["store"]
    async with sse_response(request) as res:
        request.app["consumer"].set_handler(res.send)
        await request.app["consumer"].consume()
    return res


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

    def set_handler(self, cb):
        self.send = cb

    async def consume(self):
        connection = await aio_pika.connect_robust("amqp://localhost:5672/", loop=loop)

        async with connection:
            queue_name = "test_queue"

            # Creating channel
            channel = await connection.channel()  # type: aio_pika.Channel

            # Declaring queue
            queue = await channel.declare_queue(
                queue_name, auto_delete=True
            )  # type: aio_pika.Queue

            async with queue.iterator() as queue_iter:
                # Cancel consuming after __aexit__
                async for message in queue_iter:
                    async with message.process():
                        print(message.body)

                        if queue.name in message.body.decode():
                            break

    def consume_sync(self):
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
        if method.routing_key == Routes.OBJECTS:
            self.stats_db.add_generated()
        if method.routing_key == Routes.TASK_REQUESTS:
            self.stats_db.add_distributed()
        print(f"process message {data=}")
        await self.send(json.dumps(asdict(self.stats_db)))


if __name__ == "__main__":
    id_ = uuid4().hex
    stats_db = Store()
    app = web.Application()

    p = Staistics(id_, stats_db)
    # thread = threading.Thread(target=p.consume)
    # thread.start()

    app["store"] = stats_db
    app["consumer"] = p
    app.add_routes([web.get("/", main_page)])
    app.add_routes([web.get("/updates", update_subscription)])

    asyncio.get_event_loop()

    web.run_app(app, port=8004)
