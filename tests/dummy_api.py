from lightbus import Api


class DummyApi(Api):

    class Meta:
        name = 'dummy.api'

    def my_proc(self, field):
        return 'value: '.format(field)
