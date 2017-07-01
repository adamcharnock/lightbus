import sys
import timeit

from .celery_tasks import add


TOTAL_MESSAGES = int(sys.argv[1])


def do_it():
    result = add.delay(4, 4)
    print('>', end='', flush=True)
    result.get()
    print('<', end='', flush=True)


seconds = timeit.timeit(do_it, number=TOTAL_MESSAGES) / TOTAL_MESSAGES
print('Time per call: {}ms'.format(round(seconds * 1000, 2)))
