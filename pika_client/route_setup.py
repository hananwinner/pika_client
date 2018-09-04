import threading
import time


class RouteSetup(object):
    def __init__(self, setup_config, channel, on_complete):
        self._setup_config = setup_config
        self._channel = channel
        self._on_complete = on_complete
        self._task_list = self.make_task_list(self._setup_config)
        self._task_iter = iter(self._task_list)

    @staticmethod
    def make_task_list(_setup_config):
        task_list = []
        exc = _setup_config.get("exchange")
        if exc is not None and exc != "":
            task_list.append(("exchange", exc))

        queue = _setup_config.get("queue")
        if queue is not None:
            task_list.append(("queue", queue))
        return task_list

    def _next_task(self, method_frame):
        try:
            task, name = self._task_iter.__next__()
            self._process_task(task, name)
        except StopIteration:
            self._on_complete()

    def _process_task(self, element, name):
        if element == "exchange":
            self._setup_exchange(name)
        elif element == "queue":
            self._setup_queue(name)
        else:
            raise NotImplementedError("")

    def _setup_exchange(self, exc, exc_type="direct"):
        self._channel.exchange_declare(self._next_task, exc, exc_type)

    def _setup_queue(self, queue, durable=True, passive=False):
        self._channel.queue_declare(self._next_task, queue, durable=durable, passive=passive)

    def setup(self):
        self._next_task(None)
