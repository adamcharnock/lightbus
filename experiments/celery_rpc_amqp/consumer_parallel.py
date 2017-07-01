import sys
import timeit

from .celery_tasks import add


TOTAL_MESSAGES = int(sys.argv[1])


def do_it():
    results = []
    for x in range(0, TOTAL_MESSAGES):
        results.append(add.delay(4, 4))
        print('>', end='', flush=True)

    for result in results:
        result.get()
        print('<', end='', flush=True)


seconds = timeit.timeit(do_it, number=1) / TOTAL_MESSAGES
print('Time per call: {}ms'.format(round(seconds * 1000, 2)))
