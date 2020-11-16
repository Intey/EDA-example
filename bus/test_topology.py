from bus.topology_connector import TopologyConnector


def test_init():
    topology = TopologyConnector("./topology.yaml")
    topology.providers
    # topology.init_provider('rabbitmq')
    connection = topology.get_service_connection("datagen")
    with connection:
        connection.send()
