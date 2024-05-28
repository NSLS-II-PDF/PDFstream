import typing
import uuid
from configparser import ConfigParser

from bluesky.callbacks import CallbackBase
from bluesky.callbacks.zmq import RemoteDispatcher as RemoteDispatcherZMQ
from bluesky_kafka import RemoteDispatcher as RemoteDispatcherKafka

from nslsii.kafka_utils import _read_bluesky_kafka_config_file

from pdfstream.io import server_message
from pdfstream.vend.qt_kicker import install_qt_kicker


class ServerConfig(ConfigParser):
    """The configuration for the server."""

    def __init__(self, *args, **kwargs):
        super(ServerConfig, self).__init__(*args, **kwargs)
        self.add_section("LISTEN TO")

    @property
    def host(self):
        return self.get("LISTEN TO", "host", fallback="localhost")

    @property
    def port(self):
        return self.getint("LISTEN TO", "port", fallback=5568)

    @property
    def address(self):
        return self.host, self.port

    @property
    def prefix(self):
        return self.get("LISTEN TO", "prefix", fallback="raw").encode()

    def read(self, filenames, encoding=None) -> typing.List[str]:
        returned = super(ServerConfig, self).read(filenames, encoding=encoding)
        if not returned:
            raise FileNotFoundError("No such configuration file {}".format(filenames))
        return returned


class BaseServer(RemoteDispatcherZMQ):
    """The basic server class."""

    def __init__(self, config: ServerConfig):
        super(BaseServer, self).__init__(config.address, prefix=config.prefix)
        self._config = config

    def start(self):
        try:
            server_message(
                "Server is started. " +
                "Listen to {}:{} prefix {}.".format(self._config.host, self._config.port, self._config.prefix)
            )
            super(BaseServer, self).start()
        except KeyboardInterrupt:
            server_message("Server is terminated.")

    def install_qt_kicker(self):
        install_qt_kicker(self.loop)


class BaseServerKafka(RemoteDispatcherKafka):
    """The basic server class using Kafka message bus."""

    def __init__(self, config: ServerConfig):
        self._beamline_acronym = "xpd"
        unique_group_id = f"echo-{self._beamline_acronym}-{str(uuid.uuid4())[:8]}"
        kafka_config = _read_bluesky_kafka_config_file(config_file_path="/etc/bluesky/kafka.yml")
        print(f"{kafka_config = }")
        self._topics = [f"{self._beamline_acronym}.bluesky.runengine.documents"]
        super().__init__(
            topics=self._topics,
            bootstrap_servers=",".join(kafka_config["bootstrap_servers"]),
            group_id=unique_group_id,
            consumer_config=kafka_config["runengine_producer_config"],)
        self._config = config
        self._kafka_config = kafka_config

    def start(self):
        try:
            server_message(
                "Server is started. " +
                "Listen to {}, topics {}.".format(self._kafka_config["bootstrap_servers"], self._topics)
            )
            super().start()
        except KeyboardInterrupt:
            server_message("Server is terminated.")

    def install_qt_kicker(self):
        pass


class StartStopCallback(CallbackBase):
    """Print the time for analysis"""

    def __init__(self):
        super(StartStopCallback, self).__init__()

    def start(self, doc):
        server_message("Receive the start of run {}".format(doc["uid"]))
        return super(StartStopCallback, self).start(doc)

    def descriptor(self, doc):
        server_message("Receive the stream {}.".format(doc["name"]))
        return super(StartStopCallback, self).descriptor(doc)

    def event(self, doc):
        server_message("Receive the event {}.".format(doc["seq_num"]))
        return super(StartStopCallback, self).event(doc)

    def event_page(self, doc):
        server_message("Receive the event page.")
        return super(StartStopCallback, self).event_page(doc)

    def stop(self, doc):
        server_message("Receive the stop of run {}".format(doc.get("run_start", "")))
        return super(StartStopCallback, self).stop(doc)
