import pika
from pika_client.connector import _AsyncConnector
import queue
import json
import threading


class AsyncMemQueuePublisher(_AsyncConnector):
    def __init__(self, connection_parameters, **kwargs):
        self._mem_q = kwargs.get("mem_q") or queue.Queue(maxsize=kwargs.get("maxsize", 0))
        super(AsyncMemQueuePublisher, self).__init__(connection_parameters, **kwargs)

        self._delay_ms = kwargs.get("delay_ms", 10)
        self._empty_delay_ms = kwargs.get("empty_delay_ms", 1000)
        self._default_exc = kwargs.get("exchange")
        self._default_rk = kwargs.get("routing_key")
        self._deliveries = []
        self._acked = 0
        self._nacked = 0
        self._message_number = 0
        self._delayed_stop = False
        self._flush = False
        self._flush_lock = threading.Lock()

    def start_operation(self):
        self._start_publish(self._channel)

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
        if self._flush and is_empty:
            self._flush_lock.release()
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

    def stop(self):
        if self._stopping:
            return
        else:
            super(AsyncMemQueuePublisher, self).stop()
            if self._flush:
                self._flush_lock.release()

    def flush(self, timeout=-1):
        self._flush = True
        self._flush_lock.acquire()
        all_flushed = self._flush_lock.acquire(timeout)
        self._flush = False
        return all_flushed
