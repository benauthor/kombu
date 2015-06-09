"""
kombu.transport.kafka
=====================

Kafka transport.

:copyright: (c) 2010 - 2013 by Mahendra M.
:license: BSD, see LICENSE for more details.

**Synopsis**

Connects to kafka (0.8.x) as <server>:<port>/<vhost>
The <vhost> becomes the group for all the clients. So we can use
it like a vhost

It is recommended that the queue be created in advance, by specifying the
number of partitions. The partition configuration determines how many
consumers can fetch data in parallel

**Limitations**
* The client API needs to modified to fetch data only from a single
  partition. This can be used for effective load balancing also.

"""

from __future__ import absolute_import

import socket

from anyjson import loads, dumps

from amqp import ConnectionError, ChannelError
from kombu.five import Empty

from . import virtual

try:
    import pykafka
    from pykafka import KafkaClient

    KAFKA_CONNECTION_ERRORS = ()
    KAFKA_CHANNEL_ERRORS = ()

except ImportError:
    kafka = None                                          # noqa
    KAFKA_CONNECTION_ERRORS = KAFKA_CHANNEL_ERRORS = ()   # noqa

DEFAULT_PORT = 9092

__author__ = 'Mahendra M <mahendra.m@gmail.com>'


"""
TODO some config
let us delete:
delete.topic.enable=true
MessageSizeTooLarge
http://stackoverflow.com/questions/21020347/kafka-sending-a-15mb-message
"""


class Channel(virtual.Channel):

    _client = None
    _kafka_group = None
    _kafka_consumers = {}
    _kafka_producers = {}

    def _get_producer(self, queue):
        """Create/get a producer instance for the given topic/queue"""

        producer = self._kafka_producers.get(queue, None)
        if producer is None:
            producer = self.client.topics[queue].get_producer()
            self._kafka_producers[queue] = producer

        return producer

    def _get_consumer(self, queue):
        """Create/get a consumer instance for the given topic/queue"""

        consumer = self._kafka_consumers.get(queue, None)
        if consumer is None:
            consumer = self.client.topics[queue].get_simple_consumer(
                consumer_group="temp")
            self._kafka_consumers[queue] = consumer

        return consumer

    def _put(self, queue, message, **kwargs):
        """Put a message on the topic/queue"""
        producer = self._get_producer(queue)
        producer.produce([dumps(message)])

    def _get(self, queue):
        """Get a message from the topic/queue"""
        consumer = self._get_consumer(queue)

        msg = consumer.consume(block=False)
        if not msg:
            raise Empty()

        return loads(msg.value)

    def _purge(self, queue):
        """Purge all pending messages in the topic/queue"""
        # TODO broken in lib but something like
        # consumer = self._get_consumer(queue)
        # for op, p in consumer.partitions.items():
        #     op.set_offset(p.latest_available_offsets())
        pass

    def _delete(self, queue, *args, **kwargs):
        """Delete a queue/topic"""

        # We will just let it go through. There is no API defined yet
        # for deleting a queue/topic
        pass

    def _size(self, queue):
        """Gets the number of pending messages in the topic/queue"""
        consumer = self._get_consumer(queue)
        return sum([o.message_count for o in consumer.partitions.keys()])

    def _new_queue(self, queue, **kwargs):
        """Create a new queue if it does not exist"""
        # Just create a producer, the queue will be created automatically
        self._get_producer(queue)

    def _has_queue(self, queue):
        """Check if a queue already exists"""
        return queue in self.client.topics

    def _open(self):
        conninfo = self.connection.client
        port = conninfo.port or DEFAULT_PORT
        # client = KafkaClient(hosts="%s:%s" % (conninfo.hostname, port))
        # TODO obvs don't hard code this
        client = KafkaClient(hosts="192.168.59.103:32795")
        return client

    @property
    def client(self):
        if self._client is None:
            self._client = self._open()
            self._kafka_group = self.connection.client.virtual_host[0:-1]
        return self._client


class Transport(virtual.Transport):
    Channel = Channel
    polling_interval = 1
    default_port = DEFAULT_PORT
    connection_errors = (ConnectionError, ) + KAFKA_CONNECTION_ERRORS
    channel_errors = (ChannelError, socket.error) + KAFKA_CHANNEL_ERRORS
    driver_type = 'kafka'
    driver_name = 'kafka'

    def __init__(self, *args, **kwargs):
        if pykafka is None:
            raise ImportError('The kafka library is not installed')

        super(Transport, self).__init__(*args, **kwargs)

    def driver_version(self):
        return pykafka.__version__
