from lightbus import Api, Event
from lightbus.exceptions import SuddenDeathException


class DummyApi(Api):
    my_event = Event(['field'])

    class Meta:
        name = 'my.dummy'

    def my_proc(self, field):
        return 'value: {}'.format(field)

    def sudden_death(self, n):
        # Die when n is a multiple of 5
        if int(n) % 5 == 0:
            raise SuddenDeathException()
        else:
            return n
