import argparse
import functools
import logging
import threading
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
        self.work_done_event = threading.Event()

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
        self.start_consuming()

    def on_message_callback(self, ch, method_frame, _header_frame, body):
        assert self.consume_ch is ch
        msgsize = getsizeof(body)
        logging.info("consumed message, size %d", msgsize)
        self.totalsize = self.totalsize + msgsize
        delivery_tag = method_frame.delivery_tag
        # appending if the size of data is less than 4000 bytes
        if self.totalsize < 4000:
            self.consolidated.append(body)
            self.consume_ch.basic_ack(delivery_tag)
        else:
            # stopping to consume and processing the appended data
            # reject the message that was consumed and that wasnt greater in size to be appended
            logging.info("rejecting message with tag %d", delivery_tag)
            self.consume_ch.basic_reject(delivery_tag=delivery_tag, requeue=True)
            th = threading.Thread(target=self.do_work, args=(self.consolidated,))
            th.start()
            logging.info("stopping consuming...")
            self.consume_ch.stop_consuming()

    def start_consuming(self):
        logging.info("START consuming")
        self.work_done_event.clear()
        self.totalsize = 0
        self.consolidated.clear()
        self.consume_ch.basic_qos(prefetch_count=1)
        self.consume_ch.basic_consume(
            on_message_callback=self.on_message_callback, queue=self.consume_qname
        )
        self.consume_ch.start_consuming()
        logging.info("consuming STOPPED, waiting for work to finish...")
        while not self.work_done_event.is_set():
            self.connection.process_data_events(time_limit=5)
            if self.work_done_event.is_set():
                logging.info("work is done, re-starting consuming!")
                self.start_consuming()
            else:
                logging.info("still waiting for work to finish...")

    def work_is_done(self, data):
        self.publish_ch.basic_publish(
            exchange="", routing_key=self.publish_qname, body=str(data)
        )
        logging.info("published response")

    def do_work(self, data):
        # processing data for 30 min
        # NOTE: You WILL exceeed the channel timeout here if your task takes longer than 30 minutes
        # logging.info("sleeping for 20 minutes to simulate work...")
        # time.sleep(1200)
        logging.info("sleeping for 30 seconds to simulate work...")
        time.sleep(30)
        cb = functools.partial(self.work_is_done, data)
        self.connection.add_callback_threadsafe(cb)
        logging.info("DONE - simulated work")
        self.work_done_event.set()


ob = Rabbitmq(args.host, args.port, args.vhost)
