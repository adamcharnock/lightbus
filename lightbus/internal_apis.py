from lightbus.api import Api, Event


class LightbusStateApi(Api):
    """The API for the state plugin"""

    worker_started = Event(
        parameters=[
            "service_name",
            "process_name",
            "metrics_enabled",
            "api_names",
            "listening_for",
            "timestamp",
            "ping_interval",
        ]
    )
    worker_ping = Event(
        parameters=[
            "service_name",
            "process_name",
            "metrics_enabled",
            "api_names",
            "listening_for",
            "timestamp",
            "ping_interval",
        ]
    )
    worker_stopped = Event(parameters=["process_name", "timestamp"])

    class Meta:
        name = "internal.state"
        internal = True


class LightbusMetricsApi(Api):
    """The API for the metrics plugin"""

    rpc_call_sent = Event(
        parameters=[
            "service_name",
            "process_name",
            "id",
            "api_name",
            "procedure_name",
            "kwargs",
            "timestamp",
        ]
    )
    rpc_call_received = Event(
        parameters=["service_name", "process_name", "id", "api_name", "procedure_name", "timestamp"]
    )
    rpc_response_sent = Event(
        parameters=[
            "service_name",
            "process_name",
            "id",
            "api_name",
            "procedure_name",
            "result",
            "timestamp",
        ]
    )
    rpc_response_received = Event(
        parameters=["service_name", "process_name", "id", "api_name", "procedure_name", "timestamp"]
    )

    event_fired = Event(
        parameters=[
            "service_name",
            "process_name",
            "event_id",
            "api_name",
            "event_name",
            "kwargs",
            "timestamp",
        ]
    )
    event_received = Event(
        parameters=[
            "service_name",
            "process_name",
            "event_id",
            "api_name",
            "event_name",
            "kwargs",
            "timestamp",
        ]
    )
    event_processed = Event(
        parameters=[
            "service_name",
            "process_name",
            "event_id",
            "api_name",
            "event_name",
            "kwargs",
            "timestamp",
        ]
    )

    class Meta:
        name = "internal.metrics"
        internal = True
