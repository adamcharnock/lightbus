import lightbus

bus = lightbus.create()


class MyApi(lightbus.Api):
    class Meta:
        version = 1


bus.client.register_api(MyApi())


class MyMigrations():
    def migrate_1_to_2(self, event: lightbus.EventMessage) -> lightbus.EventMessage:
        pass

    def migrate_2_to_1(self, event: lightbus.EventMessage) -> lightbus.EventMessage:
        pass

    def migrate_2_to_3(self, event: lightbus.EventMessage) -> lightbus.EventMessage:
        pass


@migrations(MyMigrations(), to_version=4)
def my_listener():
    pass


bus.api.event.listen(my_listener, listener_name="my_listener")
