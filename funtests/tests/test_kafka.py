from nose import SkipTest

from funtests import transport


class test_kafka(transport.TransportCase):
    transport = 'kafka'
    prefix = 'kafka'
    event_loop_max = 100

    def before_connect(self):
        try:
            import pykafka  # noqa
        except ImportError:
            raise SkipTest('pykafka not installed')

    def after_connect(self, connection):
        connection.channel().client
