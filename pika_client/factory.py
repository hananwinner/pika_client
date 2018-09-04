from pika_client.connector import *


def create_persistent_async_consumer(connection_parameters, setup_config, on_message_callback, **kwargs):
    return AsyncConsumer(connection_parameters, setup_config, on_message_callback, **kwargs)


def create_persistent_async_publisher(connection_parameters, setup_config, **kwargs):
    return AsyncMemQueuePublisher(connection_parameters, setup_config, **kwargs)
