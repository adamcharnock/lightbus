from time import time


class Timer(object):
    def __init__(self):
        self.totals = []
        self.stack = []

    def __enter__(self):
        self.stack.append(time())

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.totals.append(time() - self.stack.pop())

    def __str__(self):
        return '{}ms'.format(round(self.total * 1000, 2))

    @property
    def total(self):
        return sum(self.totals)
