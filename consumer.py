import argparse
import ast
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
        self.max_byte_size_to_combine = 500
        self.consolidated = []
        self.connection = None
        self.consume_ch = None
        self.publish_ch = None
        self.work_done_event = threading.Event()

        credentials = pika.PlainCredentials(username="guest", password="guest")
        parameters = pika.ConnectionParameters(
            host=host,
            port=port,
            virtual_host=vhost,
            credentials=credentials,
            heartbeat=10,
        )
        self.connection = pika.BlockingConnection(parameters=parameters)
        self.consume_ch = self.connection.channel()
        q3_test_q = self.consume_ch.queue_declare("q3_test", durable=True, arguments={"x-queue-type": "quorum"},)
        logging.info("declared queue: %s", pprint.pformat(q3_test_q))
        self.consume_qname = q3_test_q.method.queue

        self.publish_ch = self.connection.channel()
        result_queue_q = self.publish_ch.queue_declare(queue="result_queue")
        logging.info("declared queue: %s", pprint.pformat(result_queue_q))
        self.publish_qname = result_queue_q.method.queue

        self.run()

    def run(self):
        while True:
            result = self.start()
            if result:
                continue

    def start(self):
        self.work_done_event.clear()
        self.consume_ch.basic_qos(prefetch_count=1)
        self.start_consuming()

        while not self.work_done_event.is_set():
            self.connection.process_data_events(time_limit=15)
            if self.work_done_event.is_set():
                # returning true, to start the consuming again
                return True
            else:
                pass
        return True

    def start_consuming(self):
        consolidated_messages = []
        consolidated_byte_size = 0

        for method_frame, properties, body in self.consume_ch.consume(queue=self.consume_qname, inactivity_timeout=15):
            try:
                body_ = body.decode("UTF-8")
                asset = ast.literal_eval(body_)

                msg_body_size = getsizeof(asset)
                consolidated_byte_size = consolidated_byte_size + msg_body_size
                delivery_tag = method_frame.delivery_tag

                if consolidated_byte_size < self.max_byte_size_to_combine:
                    consolidated_messages.append(asset)
                    self.consume_ch.basic_ack(delivery_tag)

                elif consolidated_byte_size > self.max_byte_size_to_combine and not len(consolidated_messages):
                    consolidated_messages.append(asset)
                    self.consume_ch.basic_ack(delivery_tag)
                    # tried using self.consume_ch.cancel, here didn't seem to work
                    self.trigger_thread(consolidated_messages, self.consume_ch)

                else:
                    self.consume_ch.basic_reject(delivery_tag=delivery_tag, requeue=True)
                    # tried using self.consume_ch.cancel to stop the consumer here to, didn't seem to work
                    self.trigger_thread(consolidated_messages, self.consume_ch)


            except AttributeError:
                # if there are any messages and inactivity times out remaining messages are processed
                if not body and len(consolidated_messages):
                    self.trigger_thread(consolidated_messages, self.consume_ch)

            except Exception:
                if (body is None) and (len(consolidated_messages) == 0):
                    self.work_done_event.set()
                    return
                continue

    def trigger_thread(self, data, channel_):
        try:
            channel_.stop_consuming()
            th = threading.Thread(target=self.do_work, args=(data,))
            th.start()
        except Exception as e:
            raise e

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
        time.sleep(10)
        cb = functools.partial(self.work_is_done, data)
        self.connection.add_callback_threadsafe(cb)
        logging.info("DONE - simulated work")
        self.work_done_event.set()


ob = Rabbitmq(args.host, args.port, args.vhost)
