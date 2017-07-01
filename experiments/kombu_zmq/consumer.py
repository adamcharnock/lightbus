import sys
import timeit

import zmq
from kombu import Connection
from timer import Timer

TOTAL_MESSAGES = int(sys.argv[1])


amqp_timer = Timer()
zmq_timer = Timer()


def log(msg):
    pass
    # print(msg)


def main():
    context = zmq.Context()
    sockets = {}

    with Connection('amqp://guest:guest@127.0.0.1:5672//') as conn:
        simple_queue = conn.SimpleQueue('simple_queue')
        # Block until we get the 'ready to start' message
        print('Waiting for kick-off message from producer')
        simple_queue.get(block=True).ack()
        print("Got it! Let's go...")

        def get():
            nonlocal sockets

            with amqp_timer:
                message = simple_queue.get(block=True)
            message_id = message.headers.get('id')
            addr = message.headers.get('reply-to')

            if not addr:
                with amqp_timer:
                    message.ack()
                log('Message with no reply-to header. Ignoring.')
                return

            if addr not in sockets:
                log('Opening socket to: {}'.format(addr))
                with zmq_timer:
                    socket = context.socket(zmq.PUSH)
                    socket.connect(addr)
                sockets[addr] = socket

            socket = sockets[addr]
            log('Sending response for {} to: {}'.format(message_id, addr))

            # Send the message ID back plus some data
            with zmq_timer:
                socket.send(bytes(message_id, 'utf8') + b' x' * 1024)

            log('Sent')

            with amqp_timer:
                message.ack()

        seconds = timeit.timeit(get, number=TOTAL_MESSAGES)
        print("Time per get: {}ms".format(round(seconds * 1000 / TOTAL_MESSAGES, 2)))
        print("Gets per second: {}".format(round(TOTAL_MESSAGES / seconds, 2)))
        print("ZeroMQ time: {}".format(zmq_timer))
        print("AMQP time: {}".format(amqp_timer))
        simple_queue.close()


if __name__ == '__main__':
    main()
