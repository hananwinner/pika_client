import unittest
import yaml
from pika_client.factory import create_persistent_async_consumer, create_persistent_async_publisher
import time

with open("test/config/connectors.yaml", "r") as fdr:
    config = yaml.load(fdr)

connection_parameters = config["consumers"]["default"]["connection"]
consumer_route_setup_conf = config["consumers"]["default"]["route"]
publisher_route_setup_conf = config["publishers"]["default"]["route"]


class TestWorkQueue(unittest.TestCase):
    def setUp(self):
        self._publisher = create_persistent_async_publisher(connection_parameters, publisher_route_setup_conf)
        self._consumer = create_persistent_async_consumer(connection_parameters, consumer_route_setup_conf, self.on_message)

    def on_message(self, unused_channel, basic_deliver, properties, body):
        print(body)
        unused_channel.basic_ack(basic_deliver.delivery_tag)

    # def test_stop_publisher(self):
    #     self._stop_connector(self._publisher)
    #
    # def test_stop_consumer(self):
    #     self._stop_connector(self._consumer)
    #
    # @staticmethod
    # def _stop_connector(connector):
    #     connector.start()
    #     connector.stop()

    def test_message(self):
        self._consumer.start()
        self._publisher.start()

        self._publisher.send(
            {"product_name": "",
             "photo_url": "",
             "barcode": "",
             "sku": "",
             "price_cents": "",
             "producer": ""
             },
            exchange=config["publishers"]["default"]["route"]["exchange"],
            routing_key=config["publishers"]["default"]["route"]["routing_key"]
        )
        time.sleep(10)


