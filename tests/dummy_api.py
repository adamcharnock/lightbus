import logging
from random import random

from lightbus import Api, Event, Parameter
from lightbus.exceptions import SuddenDeathException

logger = logging.getLogger(__name__)


class DummyApi(Api):
    my_event = Event([Parameter("field", str)])

    class Meta:
        name = "my.dummy"

    def my_proc(self, field) -> str:
        return "value: {}".format(field)

    def sudden_death(self, n):
        raise SuddenDeathException()

    def random_death(self, n, death_every=2):
        if n % death_every == 0:
            logger.warning(f"Triggering SuddenDeathException. n={n}")
            raise SuddenDeathException()
        return n

    def general_error(self):
        raise RuntimeError("Oh no, there was some kind of error")
