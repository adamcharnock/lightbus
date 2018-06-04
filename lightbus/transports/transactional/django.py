import asyncio

import lightbus.bus
from lightbus.transports.transactional import lightbus_set_database, DbApiConnection
from django.db import connections

from lightbus.utilities.async import block


class TransactionTransportMiddleware(object):

    def __init__(self, get_response=None):
        self.get_response = get_response
        bus_module = lightbus.bus.import_bus_py()
        self.bus = bus_module.bus
        self.loop = asyncio.get_event_loop()
        self.migrate()

    def migrate(self):
        # TODO: This needs to be a core lightbus feature somehow
        with connections["default"].cursor() as cursor:
            block(DbApiConnection(connections["default"], cursor).migrate(), self.loop, timeout=5)

    def __call__(self, request):
        lightbus_transaction_context = lightbus_set_database(self.bus, connections["default"])
        block(lightbus_transaction_context.__aenter__(), self.loop, timeout=5)

        response = self.get_response(request)

        if 500 <= response.status_code < 600:
            block(lightbus_transaction_context.__aexit__(True, True, True), self.loop, timeout=5)
        else:
            block(lightbus_transaction_context.__aexit__(None, None, None), self.loop, timeout=5)

        return response
