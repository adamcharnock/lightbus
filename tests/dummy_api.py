from lightbus import Api, Event


class DummyApi(Api):
    my_event = Event(['field'])

    class Meta:
        name = 'my.dummy'

    def my_proc(self, field):
        return 'value: {}'.format(field)
