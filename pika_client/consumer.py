from pika_client.connector import _AsyncConnector


class AsyncConsumer(_AsyncConnector):
    def __init__(self, connection_parameters, on_message, **kwargs):
        self._queue = kwargs.get("queue")
        self._on_message = self._on_message_ack_decorator(on_message)
        super(AsyncConsumer, self).__init__(connection_parameters, **kwargs)
        self._consumer_tag = None

    def start_operation(self):
        self._start_consume(self._channel)

    def _on_message_ack_decorator(self, on_message):
        def __on_message_ack_decorator(channel, basic_deliver, properties, body):
            try:
                on_message(body)
                channel.basic_ack(basic_deliver.delivery_tag)
            except:
                channel.basic_nack(basic_deliver.delivery_tag)
        return __on_message_ack_decorator

    def _start_consume(self, channel):
        self._channel.add_on_cancel_callback(self.on_consumer_cancelled)
        self._consumer_tag = self._channel.basic_consume(self._on_message, self._queue)

    def on_consumer_cancelled(self, method_frame):
        if self._channel:
            self._channel.close()

    def stop(self):
        if self._stopping:
            return
        self._stopping = True
        self._stop_consuming()
        self._connection.ioloop.start()

    def _stop_consuming(self):
        if self._channel:
            self._channel.basic_cancel(self._on_cancelok, self._consumer_tag)

    def _on_cancelok(self, unused_frame):
        self._channel.close()
