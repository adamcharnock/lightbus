import lightbus

bus = lightbus.create()


class FirstApi(lightbus.Api):

    class Meta:
        name = "first"
