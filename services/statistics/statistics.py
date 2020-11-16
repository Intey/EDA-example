import json
import time
from uuid import uuid4
from aiohttp import web
import threading
from aiohttp_sse import sse_response
import asyncio
import kafka
import sys
from memory import Store


class Routes:
    TASK_REQUESTS = "tasks-requests"
    PROCESSING = "processing"
    OBJECTS = "objects"


start = "START_PROCESSING"
finish = "END_PROCESSING"
distribution = "DISTRIBUTION"
generation = "GENERATE_OBJECT"

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


class MQKafka:
    def __init__(self, consume_route: str, group_id: str = None):
        self.group_id = group_id
        self.consume_route = consume_route
        self.producer = kafka.KafkaProducer(
            bootstrap_servers="localhost:9092",
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        self.consumer = self.consumer = kafka.KafkaConsumer(
            group_id=self.group_id, value_deserializer=self.decode_message,
        )
        print("subscribed to all topics")
        self.consumer.subscribe(
            [Routes.TASK_REQUESTS, Routes.PROCESSING, Routes.OBJECTS]
        )
        print("subscribed.")

    def send(self, obj: dict, route: str):
        f = self.producer.send(route, value=obj)
        f.get(timeout=20)

    async def accept(self, callback):
        for msg in self.consumer:
            print("accept message", msg.value, "wait callback")
            await callback(msg.value)

    @staticmethod
    def decode_message(msg):
        try:
            return json.loads(msg.decode("utf-8"))
        except json.JSONDecodeError:
            data = msg.decode("utf-8")
            print("failed process message", data)
            return {"type": "unknown", "data": data}


async def main_page(request):
    stats = request.app["store"]
    html = MAIN_HTML.replace("{{body}}", str(stats))
    return web.Response(text=html, content_type="text/html")


async def update_subscription(request):
    print("subscribe ES")
    store = request.app["store"]
    consumer = request.app["consumer"]
    loop = request.app.loop
    async with sse_response(request) as res:
        print("set handler")
        consumer.set_handler(res.send)
        print("wait messages...")
        count = 1
        while True:
            # for msg in consumer.connector.consumer:
            # payload = json.dumps(msg.value)
            count += 1
            msg = dict(count=count)
            await res.send(json.dumps(count))
            await asyncio.sleep(1, loop=loop)
    return res


class Routes:
    OBJECTS = "objects"
    TASK_REQUESTS = "tasks-requests"
    PROCESSING = "processing"


class MessageTypes:
    start = "START_PROCESSING"
    finish = "END_PROCESSING"


class Staistics:
    def __init__(self, id_: str, stats_db: Store):
        self.id = id_
        self.stats_db = stats_db
        self.connector = MQKafka(".", "statistics")
        self.hndl = None

    def set_handler(self, cb):
        if self.hndl is None:
            self.hndl = cb

    async def consume(self):
        await self.connector.accept(self.callback)

    async def callback(self, data: dict):
        if self.hndl:
            await self.hndl(json.dumps(data))
        msg_type = data.get("type")
        print(f"process {msg_type=} in callback")
        if msg_type == finish:
            self.stats_db.finish_process_one()
        elif msg_type == start:
            self.stats_db.start_process_one()
        elif msg_type == generation:
            self.stats_db.add_generated()
        elif msg_type == distribution:
            self.stats_db.add_distributed()


if __name__ == "__main__":
    id_ = uuid4().hex
    stats_db = Store()
    app = web.Application()

    p = Staistics(id_, stats_db)
    # thread = threading.Thread(target=p.consume)
    # thread.start()

    # def quit():
    #     thread.interrupt()
    #     sys.quit()
    # import signal
    # signal.signal(2, quit)

    app["store"] = stats_db
    app["consumer"] = p
    print("prepare for consuming")
    app.add_routes([web.get("/", main_page)])
    app.add_routes([web.get("/updates", update_subscription)])

    print("web app started")
    web.run_app(app, port=8004)
