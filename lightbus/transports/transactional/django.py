import asyncio

import lightbus.client
import lightbus.creation
from lightbus.transports.transactional import lightbus_set_database, DbApiConnection
from django.db import connections

from lightbus.utilities.async import block


class TransactionTransportMiddleware(object):

    # Note this is not thread-safe. In fact, lightbus' use of the
    # event loop may not be thread safe at all.
    def __init__(self, get_response=None):
        self.get_response = get_response
        bus_module = lightbus.creation.import_bus_module()
        self.bus = bus_module.bus
        self.migrate()

    @property
    def loop(self):
        try:
            return asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            return loop

    def migrate(self):
        # TODO: This needs to be a core lightbus feature somehow
        with connections["default"].cursor() as cursor:
            block(DbApiConnection(connections["default"], cursor).migrate(), timeout=5)

    def __call__(self, request):
        connection = connections["default"]
        if connection.in_atomic_block:
            start_transaction = False
        elif connection.autocommit:
            start_transaction = True
        else:
            start_transaction = None

        lightbus_transaction_context = lightbus_set_database(self.bus, connection)
        block(lightbus_transaction_context.__aenter__(), timeout=5)

        response = self.get_response(request)

        if 500 <= response.status_code < 600:
            block(lightbus_transaction_context.__aexit__(True, True, True), timeout=5)
        else:
            block(lightbus_transaction_context.__aexit__(None, None, None), timeout=5)

        return response
