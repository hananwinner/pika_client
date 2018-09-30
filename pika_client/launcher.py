from pika_client.route_setup import RouteSetup


class Launcher(object):
    def __init__(self, connector, route_config=None):
        self._connector = connector
        self._route_config = route_config

    def _launch(self):
        self._connector.add_listener(self)
        self._connector.start()

    def _on_channel_open(self, channel):
        if self._route_config is not None:
            route_setup = RouteSetup(self._route_config, channel, self._on_route_setup_complete)
            route_setup.setup()
        else:
            self._on_route_setup_complete()

    def _on_route_setup_complete(self):
        self._connector.start_operation()

    def start(self):
        self._launch()

    def __getattr__(self, name):
        return getattr(self._connector, name)
