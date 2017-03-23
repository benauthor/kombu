"""
Kafka transport

Exchange is a topic
Queue is a consumer group
Routing Key -- maybe we can get kafka's message key to work?
??? or something
"""
from __future__ import absolute_import, unicode_literals

from kafka import KafkaProducer

from kombu.five import Queue, values

from . import base
from . import virtual

DEFAULT_PORT = 9092


class Channel(virtual.Channel):

    def __init__(self, connection, **kwargs):
        super(Channel, self).__init__(connection, **kwargs)
        self._connection = connection
        self._producer = None
        self._consumer = None

    def _get_connect_string(self):
        host = self._connection.client.hostname or "localhost"
        port = self._connection.default_port
        return "%s:%s" % (host, port)

    def _get_producer(self):
        if self._producer is None:
            self._producer = KafkaProducer(
                bootstrap_servers=self._get_connect_string()
            )
        return self._producer

    def _get(self, queue, timeout=None):
        """Get next message from `queue`."""
        raise NotImplementedError('Virtual channels must implement _get')

    def _put(self, queue, message):
        """Put `message` onto `queue`."""
        producer = self._get_producer()
        producer.send(queue, value=message)

    def _purge(self, queue):
        """Remove all messages from `queue`."""
        raise NotImplementedError('Virtual channels must implement _purge')


class Transport(virtual.Transport):

    Channel = Channel

    default_port = DEFAULT_PORT

    # TODO override: state

    implements = base.Transport.implements

    driver_type = 'kafka'
    driver_name = 'kafka'

    def driver_version(self):
        return '0.0-dev'
