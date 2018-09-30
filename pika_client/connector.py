import pika
import threading


class _AsyncConnector(threading.Thread):
    def __init__(self, connection_parameters, **kwargs):
        super(_AsyncConnector, self).__init__()
        user = connection_parameters.pop("user", "guest")
        password = connection_parameters.pop("password", "guest")
        connection_parameters["credentials"] = pika.PlainCredentials(username=user, password=password)
        self._connection_parameters = connection_parameters
        self._connection = None
        self._channel = None
        self._stopping = False
        self._listeners = []

    def add_listener(self, l):
        self._listeners.append(l)

    def remove_listener(self, l):
        self._listeners.remove(l)

    def _on_connection_open(self, connection):
        self._connection.channel(on_open_callback=self._on_channel_open)

    def _event_on_channel_open(self, channel):
        for l in self._listeners:
            l._on_channel_open(channel)

    def _on_channel_open(self, channel):
        self._channel = channel
        self._channel.add_on_close_callback(self._on_channel_closed)
        self._event_on_channel_open(channel)

    def _on_channel_closed(self, channel, reply_code, reply_text):
        self._close_connection()

    def start_operation(self):
        raise NotImplementedError("must implement in derived class")

    def run(self):
        self._connection = pika.SelectConnection(
            pika.ConnectionParameters(**self._connection_parameters),
            self._on_connection_open, self.on_open_error, self.on_connection_closed,
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
        if self._stopping:
            return
        self._stopping = True
        self._close_connection()
