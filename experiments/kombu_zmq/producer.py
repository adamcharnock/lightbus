import sys
import os
import timeit
from random import randint
from socket import gethostbyname, gethostname

import zmq
from kombu import Connection
from timer import Timer

TOTAL_MESSAGES = int(sys.argv[1])

context = zmq.Context()
socket = context.socket(zmq.PULL)
PORT = socket.bind_to_random_port('tcp://*')
HOST = gethostbyname(gethostname())


amqp_timer = Timer()
zmq_timer = Timer()


def log(msg):
    pass
    # print(msg)

log('Listening for results on {}:{}'.format(HOST, PORT))

with Connection('amqp://guest:guest@localhost:5672//') as conn:
    simple_queue = conn.SimpleQueue('simple_queue')
    simple_queue.put('kick-off!')

    def put():
        message_id = str(randint(100, 999))
        with amqp_timer:
            simple_queue.put(b'x' * 1024, headers={
                'reply-to': 'tcp://{}:{}'.format(HOST, PORT),
                'id': message_id
            })

        log('Message {} has been put. Waiting for response...'.format(message_id))
        with zmq_timer:
            content = socket.recv()

        # Make sure we got the response for the message we sent
        assert message_id == str(content, 'utf8').split(' ', 1)[0]

        log('Response received')

    seconds = timeit.timeit(put, number=TOTAL_MESSAGES)
    print('Time per put: {}ms'.format(round(seconds * 1000 / TOTAL_MESSAGES, 2)))
    print("Puts per second: {}".format(round(TOTAL_MESSAGES / seconds, 2)))
    print("ZeroMQ time: {}".format(zmq_timer))
    print("AMQP time: {}".format(amqp_timer))
    simple_queue.close()
