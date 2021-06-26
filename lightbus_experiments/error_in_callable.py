import lightbus

bus = lightbus.create()


@bus.client.every(seconds=1)
def do_it():
    raise RuntimeError("Oh no! The kittens are escaping! 🐈🐈🐈")
