import argparse
import pika
import pprint
import logging
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

credentials = pika.PlainCredentials(username="guest", password="guest")

parameters = pika.ConnectionParameters(
    host=args.host,
    port=args.port,
    virtual_host=args.vhost,
    credentials=credentials,
    heartbeat=5,
)

connection = pika.BlockingConnection(parameters=parameters)

pub_ch = connection.channel()
consume_ch = connection.channel()

m = []

q3_test_q = pub_ch.queue_declare(queue="q3_test", durable=True)
logging.info("declared queue: %s", pprint.pformat(q3_test_q))
publish_qname = q3_test_q.method.queue

result_queue_q = consume_ch.queue_declare(queue="result_queue")
logging.info("declared queue: %s", pprint.pformat(result_queue_q))
result_qname = result_queue_q.method.queue
consume_ch.basic_qos(prefetch_count=10)


def publish_():
    msgcount = 9
    msg = "{9000: {'name': ['Antonio Klein', 'Shannon Buchanan MD', 'Sarah Patterson', 'Nicole Cooper', 'Angela Garrett'], " \
          "'email': ['waltonmitchell@example.com', 'edalton@example.com', 'shannonevans@example.org', 'jacksonkathryn@example.org', 'wendy22@example.net']}}"
    logging.info("publishing %d messages", msgcount)
    for _ in range(msgcount):
        pub_ch.basic_publish(exchange='',routing_key=publish_qname,body=msg)
    logging.info("DONE publishing %d messages", msgcount)

def consume_():
    for m, p, b in consume_ch.consume("result_queue"):
        print("received")
        consume_ch.basic_ack(m.delivery_tag)


publish_()
consume_()

pub_ch.close()
consume_ch.close()
connection.close()
