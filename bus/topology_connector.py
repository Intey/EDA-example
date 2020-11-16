from yaml import load


class TopologyConnector:
    def __init__(self, filepath):
        super().__init__()
        with open(filepath) as f:
            self.__topology = load(filepath)
            self.services = list(self.__topology["topology"]["specs"].keys())


class RabbitMQProvider:
    connection = None
    channel = None

    def send(self, msg):
        pass

    def consume(self):
        pass
