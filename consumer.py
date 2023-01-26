import argparse
import logging
import pika
import pprint
import time
from sys import getsizeof

LOG_FORMAT = (
    "%(levelname) -10s %(asctime)s %(name) -30s %(funcName) "
    "-35s %(lineno) -5d: %(message)s"
)
LOGGER = logging.getLogger(__name__)

logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

parser = argparse.ArgumentParser(add_help=False)
parser.add_argument(
    "-h",
    "--host",
    dest="host",
    default="localhost",
    help="RabbitMQ host name",
)
parser.add_argument(
    "-p",
    "--port",
    dest="port",
    default=5672,
    type=int,
    help="RabbitMQ port",
)
parser.add_argument(
    "-v",
    "--vhost",
    dest="vhost",
    default="/",
    help="RabbitMQ vhost",
)
args = parser.parse_args()


class Rabbitmq:
    def __init__(self, host, port, vhost):
        self.consolidated = []

        credentials = pika.PlainCredentials(username="guest", password="guest")
        parameters = pika.ConnectionParameters(
            host=host,
            port=port,
            virtual_host=vhost,
            credentials=credentials,
            heartbeat=5,
        )
        self.connection = pika.BlockingConnection(parameters=parameters)
        self.consume_ch = self.connection.channel()
        q3_test_q = self.consume_ch.queue_declare("q3_test", durable=True)
        logging.info("declared queue: %s", pprint.pformat(q3_test_q))
        self.consume_qname = q3_test_q.method.queue

        self.publish_ch = self.connection.channel()
        result_queue_q = self.publish_ch.queue_declare(queue="result_queue")
        logging.info("declared queue: %s", pprint.pformat(result_queue_q))
        self.publish_qname = result_queue_q.method.queue

        self.consume_()

    def consume_(self):
        logging.info("START consumer")
        self.consume_ch.basic_qos(prefetch_count=1)
        totalsize = 0
        for method_frame, props, body in self.consume_ch.consume(
            queue=self.consume_qname
        ):
            msgsize = getsizeof(body)
            logging.info("consumed message, size %d", msgsize)
            totalsize = totalsize + msgsize
            # appending if the size of data is less than 4000 bytes
            if totalsize < 4000:
                self.consolidated.append(body)
                self.consume_ch.basic_ack(method_frame.delivery_tag)
            else:
                # stopping to consume and processing the appended data
                self.consume_ch.stop_consuming()
                self.scan_data(self.consolidated)
                self.consolidated.clear()
                self.consume_()

    def scan_data(self, data):
        # processing data for 30 min
        # NOTE: You WILL exceeed the channel timeout here if your task takes longer than 30 minutes
        logging.info("sleeping for 10 seconds to simulate work...")
        time.sleep(10)
        # publishing
        self.publish_ch.basic_publish(
            exchange="", routing_key=self.publish_qname, body=str(data)
        )
        logging.info("published response")


ob = Rabbitmq(args.host, args.port, args.vhost)
