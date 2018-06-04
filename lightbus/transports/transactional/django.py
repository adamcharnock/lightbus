from lightbus.transports.transactional import lightbus_atomic


class TransactionTransportMiddleware:

    def process_view(self, request, view_func, view_args, view_kwargs):
        lightbus_atomic()

    def process_response(self, request, view_func, view_args, view_kwargs):
        pass
