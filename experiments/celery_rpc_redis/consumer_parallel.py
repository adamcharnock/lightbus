import sys
import timeit

from .celery_tasks import test_task


TOTAL_MESSAGES = int(sys.argv[1])


def do_it():
    results = []
    for x in range(0, int(TOTAL_MESSAGES)):
        results.append(test_task.delay())

    for result in results:
        result.get()

do_it()

seconds = timeit.timeit(do_it, number=1) / TOTAL_MESSAGES
print('Time per call: {}ms'.format(round(seconds * 1000, 2)))
