import sys
import timeit

from .celery_tasks import test_task


TOTAL_MESSAGES = int(sys.argv[1])


def do_it():
    for x in range(0, TOTAL_MESSAGES):
        result = test_task.delay()
        result.get()

seconds = timeit.timeit(do_it, number=1) / TOTAL_MESSAGES
print('Time per call: {}ms'.format(round(seconds * 1000, 2)))
