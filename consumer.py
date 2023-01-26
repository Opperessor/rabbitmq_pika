import functools
from sys import getsizeof
import pika
import threading
import time


class Rabbitmq:

    def __init__(self):
        self.consolidated = []

        credentials = pika.PlainCredentials(username='guest',
                                            password='guest')
        parameters = pika.ConnectionParameters(host='localhost',
                                               port=int('15672'),
                                               virtual_host='test',
                                               credentials=credentials,
                                               heartbeat=5)
        self.connection = pika.BlockingConnection(parameters=parameters)
        self.channel = self.connection.channel()
        self.Queue = self.channel.queue_declare("q3_test", durable=True)
        self.consume_()

    def consume_(self):
        for method_frame, props, body in self.channel.consume(queue="q3_test"):
            # appending if the size of data is less than 4000 bytes
            if getsizeof(self.consolidated.append(body)) < 4000:
                self.consolidated.append(body)
                self.channel.basic_ack(method_frame.delivery_tag)
            else:
                # stopping to consume and processing the appended data
                self.channel.stop_consuming()
                self.scan_data(self.consolidated)

    def scan_data(self, data):
        # processing data for 30 min
        time.sleep(1800)
        # publishing
        self.channel.basic_publish(exchange="", routing_key="result_queue", body=str(data))
        print("published")
        self.consolidated.clear()
        self.consume_()


ob = Rabbitmq()
