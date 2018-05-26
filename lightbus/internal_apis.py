from lightbus.api import Api, Event


class LightbusStateApi(Api):
    server_started = Event(
        parameters=[
            "process_name",
            "metrics_enabled",
            "api_names",
            "listening_for",
            "timestamp",
            "ping_interval",
        ]
    )
    server_ping = Event(
        parameters=[
            "process_name",
            "metrics_enabled",
            "api_names",
            "listening_for",
            "timestamp",
            "ping_interval",
        ]
    )
    server_stopped = Event(parameters=["process_name", "timestamp"])

    metrics_enabled = Event(parameters=["process_name", "timestamp"])
    metrics_disabled = Event(parameters=["process_name", "timestamp"])

    api_registered = Event(parameters=["process_name", "api_name", "timestamp"])
    api_deregistered = Event(parameters=["process_name", "api_name", "timestamp"])

    listening_started = Event(parameters=["process_name", "api_name", "event_name", "timestamp"])
    listening_stopped = Event(parameters=["process_name", "api_name", "event_name", "timestamp"])

    class Meta:
        name = "internal.state"
        internal = True
        auto_register = False


class LightbusMetricsApi(Api):
    rpc_call_sent = Event(
        parameters=["process_name", "id", "api_name", "procedure_name", "kwargs", "timestamp"]
    )
    rpc_call_received = Event(
        parameters=["process_name", "id", "api_name", "procedure_name", "timestamp"]
    )
    rpc_response_sent = Event(
        parameters=["process_name", "id", "api_name", "procedure_name", "result", "timestamp"]
    )
    rpc_response_received = Event(
        parameters=["process_name", "id", "api_name", "procedure_name", "timestamp"]
    )

    event_fired = Event(
        parameters=["process_name", "event_id", "api_name", "event_name", "kwargs", "timestamp"]
    )
    event_received = Event(
        parameters=["process_name", "event_id", "api_name", "event_name", "kwargs", "timestamp"]
    )
    event_processed = Event(
        parameters=["process_name", "event_id", "api_name", "event_name", "kwargs", "timestamp"]
    )

    class Meta:
        name = "internal.metrics"
        internal = True
        auto_register = False
