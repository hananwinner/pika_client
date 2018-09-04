import pika
from pika_client import route_setup
import threading
import queue
import json


class _AsyncConnector(threading.Thread):
    def __init__(self, connection_parameters, setup_config, on_setup_complete, **kwargs):
        super(_AsyncConnector, self).__init__()
        user = connection_parameters.pop("user", "guest")
        password = connection_parameters.pop("password", "guest")
        connection_parameters["credentials"] = pika.PlainCredentials(username=user, password=password)
        self._connection_parameters = connection_parameters
        self._setup_config = setup_config
        self.__on_setup_complete = on_setup_complete
        self._connection = None
        self._channel = None
        self._stopping = False

    def _on_connection_open(self, connection):
        self._connection.add_on_close_callback(self.on_connection_closed)
        self._connection.add_on_open_error_callback(self.on_open_error)
        self._connection.channel(on_open_callback=self._on_channel_open)

    def _on_channel_open(self, channel):
        self._channel = channel
        self._channel.add_on_close_callback(self._on_channel_closed)
        if self._setup_config is None:
            self._on_setup_complete()
        else:
            route_setup.RouteSetup(self._setup_config, self._channel, self._on_setup_complete).setup()

    def _on_channel_closed(self, channel, reply_code, reply_text):
        self._close_connection()

    def _on_setup_complete(self):
        self.__on_setup_complete(self._channel)

    def run(self):


        self._connection = pika.SelectConnection(
            pika.ConnectionParameters(**self._connection_parameters),
            self._on_connection_open,
            stop_ioloop_on_close=False)
        self._connection.ioloop.start()

    def on_connection_closed(self, connection, reply_code, reply_text):
        self._connection.ioloop.stop()

    def on_open_error(self, connection, reply_code, reply_text):
        self._connection.ioloop.stop()

    def _close_connection(self):
        if self._connection is not None:
            self._connection.close()

    def stop(self):
        self._stopping = True
        self._close_connection()


class AsyncConsumer(_AsyncConnector):
    def __init__(self, connection_parameters, setup_config, on_message, **kwargs):
        self._queue = setup_config.get("queue")
        self._on_message = self._on_message_ack_decorator(on_message)
        super(AsyncConsumer, self).__init__(connection_parameters, setup_config, self._start_consume, **kwargs)
        self._consumer_tag = None

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
        self._stopping = True
        self._stop_consuming()
        self._connection.ioloop.start()

    def _stop_consuming(self):
        if self._channel:
            self._channel.basic_cancel(self._on_cancelok, self._consumer_tag)

    def _on_cancelok(self, unused_frame):
        self._channel.close()


class AsyncMemQueuePublisher(_AsyncConnector):
    def __init__(self, connection_parameters, setup_config, **kwargs):
        self._mem_q = kwargs.get("mem_q") or queue.Queue(maxsize=kwargs.get("maxsize", 0))
        super(AsyncMemQueuePublisher, self).__init__(connection_parameters, setup_config,
                                                     self._start_publish, **kwargs)

        self._delay_ms = kwargs.get("delay_ms", 10)
        self._empty_delay_ms = kwargs.get("empty_delay_ms", 1000)
        self._default_exc = kwargs.get("exchange")
        self._default_rk = kwargs.get("routing_key")
        self._deliveries = []
        self._acked = 0
        self._nacked = 0
        self._message_number = 0

    def send(self, message, exchange=None, routing_key=None):
        if exchange is None:
            if self._default_exc is not None:
                exchange = self._default_exc
            else:
                raise ValueError("must specify exchange if default exchange not set.")
        if routing_key is None:
            if self._default_rk is not None:
                routing_key = self._default_rk
            else:
                raise ValueError("must specify routing key if default routing key not set.")
        self._mem_q.put((message, exchange, routing_key))

    def _on_delivery_confirmation(self, method_frame):
        confirmation_type = method_frame.method.NAME.split('.')[1].lower()
        if confirmation_type == 'ack':
            self._acked += 1
        elif confirmation_type == 'nack':
            self._nacked += 1
        self._deliveries.remove(method_frame.method.delivery_tag)

    def _publish(self, m, exc, rk):
        properties = pika.BasicProperties(app_id='example-publisher',
                                          content_type='application/json',
                                          )
        self._channel.basic_publish(exc, rk, json.dumps(m, ensure_ascii=False), properties)
        self._message_number += 1
        self._deliveries.append(self._message_number)

    def _start_publish(self, channel):
        self._channel.confirm_delivery(self._on_delivery_confirmation)
        self._schedule_next_message()

    def _schedule_next_message(self, is_empty=False):
        self._connection.add_timeout(
            (self._delay_ms if not is_empty else self._empty_delay_ms) / 1000.0,
            self._on_publish_scheduled)

    def _on_publish_scheduled(self):
        if self._stopping:
            return

        try:
            m, exc, rk = self._mem_q.get(block=False)
            self._publish(m, exc, rk)
            self._schedule_next_message()
        except queue.Empty:
            self._schedule_next_message(is_empty=True)