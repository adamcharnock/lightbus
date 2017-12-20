from lightbus import Api


class DummyApi(Api):

    class Meta:
        name = 'my.dummy'

    def my_proc(self, field):
        return 'value: {}'.format(field)
