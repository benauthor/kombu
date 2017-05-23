"""
Kafka transport

Exchange is a topic
Queue is a consumer group
Routing Key -- maybe we can get kafka's message key to work?
??? or something

some prior art https://github.com/BenjaminDavison/kombu/pull/1

TODO queue naming needs to serialize/deser into something kafka legal
http://stackoverflow.com/questions/37062904/what-are-apache-kafka-topic-name-limitations
pressingly, @ sign
"""
from __future__ import absolute_import, unicode_literals

from kafka import KafkaProducer, KafkaConsumer

from kombu.five import Queue, values, Empty

from . import base
from . import virtual

DEFAULT_PORT = 9092

_CONSUMER_GROUP = "kombu"


class Channel(virtual.Channel):

    def __init__(self, connection, **kwargs):
        super(Channel, self).__init__(connection, **kwargs)
        self._connection = connection
        self._producer = None
        self._consumers = {}

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

    def _get_consumer(self, queue):
        if queue not in self._consumers:
            self._consumers[queue] = KafkaConsumer(
                queue,
                bootstrap_servers=self._get_connect_string(),
                # TODO this will differ depending on fanout
                # or otherwise
                # group=_CONSUMER_GROUP,
            )
        return self._consumers[queue]

    def _get(self, queue, timeout=None):
        """Get next message from `queue`."""
        # TODO -internal batching, is this efficient,
        # maybe try a few times?
        try:
            return next(self._get_consumer(queue))
        except StopIteration:
            raise Empty()

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
