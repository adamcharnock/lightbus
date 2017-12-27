from random import random

from lightbus import Api, Event
from lightbus.exceptions import SuddenDeathException


class DummyApi(Api):
    my_event = Event(['field'])

    class Meta:
        name = 'my.dummy'

    def my_proc(self, field):
        return 'value: {}'.format(field)

    def sudden_death(self, n):
        raise SuddenDeathException()

    def random_death(self, n, death_probability=0.5):
        if random() < float(death_probability):
            raise SuddenDeathException()
        return n
