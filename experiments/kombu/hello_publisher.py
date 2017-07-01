import sys
import timeit
from kombu import Connection

TOTAL_MESSAGES = int(sys.argv[1])

with Connection('amqp://guest:guest@localhost:5672//') as conn:
    simple_queue = conn.SimpleQueue('simple_queue')
    # Let the consumer know we are ready to start
    simple_queue.put('x')

    def put():
        simple_queue.put('x' * 1024)

    seconds = timeit.timeit(put, number=TOTAL_MESSAGES) / TOTAL_MESSAGES
    print('Time per put: {}ms'.format(round(seconds * 1000, 2)))
    simple_queue.close()
