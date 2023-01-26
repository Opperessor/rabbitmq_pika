import pika
from sys import getsizeof
credentials = pika.PlainCredentials(username='guest',
                                    password='guest')
parameters = pika.ConnectionParameters(host='localhost',
                                       port=int('15672'),
                                       virtual_host='test',
                                       credentials=credentials,
                                       heartbeat=5)
connection = pika.BlockingConnection(parameters=parameters)

channel = connection.channel()
m = []
channel.queue_declare(queue="q3_test",durable=True)
channel.queue_declare(queue="result_queue")


def publish_():
    n = ''
    for _ in range(10):
        for _ in range(1000):
            n = n + 'a'
        m.append(n)
        n = ''
        channel.basic_publish(exchange="",routing_key="q3_test",body=str(m))

def consume_():
    for m, p,b in channel.consume("result_queue"):
        print("received")
        channel.basic_ack(m.delivery_tag)

publish_()
consume_()
