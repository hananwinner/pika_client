from pika_client.consumer import AsyncConsumer
from pika_client.publisher import AsyncMemQueuePublisher
from pika_client.launcher import Launcher


def _make_launcher_proxy(connector, setup_config):
    launcher = Launcher(connector, setup_config)
    return launcher


def create_persistent_async_consumer(connection_parameters, setup_config, on_message_callback, **kwargs):
    queue = setup_config.get("queue")
    connector = AsyncConsumer(connection_parameters, on_message_callback, queue=queue, **kwargs)
    return _make_launcher_proxy(connector, setup_config)


def create_persistent_async_publisher(connection_parameters, setup_config, **kwargs):
    connector = AsyncMemQueuePublisher(connection_parameters, **kwargs)
    return _make_launcher_proxy(connector, setup_config)
