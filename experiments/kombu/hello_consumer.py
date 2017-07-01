import sys
import timeit
from kombu import Connection

TOTAL_MESSAGES = int(sys.argv[1])

with Connection('amqp://guest:guest@127.0.0.1:5672//') as conn:
    simple_queue = conn.SimpleQueue('simple_queue')
    # Block until we get the 'ready to start' message
    simple_queue.get(block=True)

    def get():
        simple_queue.get(block=True, timeout=0.1).ack()

    seconds = timeit.timeit(get, number=TOTAL_MESSAGES) / TOTAL_MESSAGES
    print('Time per get: {}ms'.format(round(seconds * 1000, 2)))
    simple_queue.close()
