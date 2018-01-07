from lightbus.api import Api, Event


class LightbusStateApi(Api):
    server_started = Event(arguments=[
        'process_name', 'metrics_enabled', 'api_names', 'listening_for', 'timestamp', 'ping_interval',
    ])
    server_ping = Event(arguments=[
        'process_name', 'metrics_enabled', 'api_names', 'listening_for', 'timestamp', 'ping_interval',
    ])
    server_stopped = Event(arguments=['process_name', 'timestamp'])

    metrics_enabled = Event(arguments=['process_name', 'timestamp'])
    metrics_disabled = Event(arguments=['process_name', 'timestamp'])

    api_registered = Event(arguments=['process_name', 'api_name', 'timestamp'])
    api_deregistered = Event(arguments=['process_name', 'api_name', 'timestamp'])

    listening_started = Event(arguments=['process_name', 'api_name', 'event_name', 'timestamp'])
    listening_stopped = Event(arguments=['process_name', 'api_name', 'event_name', 'timestamp'])

    class Meta:
        name = 'internal.state'
        internal = True
        auto_register = False


class LightbusMetricsApi(Api):
    rpc_call_sent = Event(arguments=['process_name', 'rpc_id', 'api_name', 'procedure_name', 'kwargs', 'timestamp'])
    rpc_call_received = Event(arguments=['process_name', 'rpc_id', 'api_name', 'procedure_name', 'timestamp'])
    rpc_response_sent = Event(arguments=['process_name', 'rpc_id', 'api_name', 'procedure_name', 'result', 'timestamp'])
    rpc_response_received = Event(arguments=['process_name', 'rpc_id', 'api_name', 'procedure_name', 'timestamp'])

    event_fired = Event(arguments=['process_name', 'event_id', 'api_name', 'event_name', 'kwargs', 'timestamp'])
    event_received = Event(arguments=['process_name', 'event_id', 'api_name', 'event_name', 'kwargs', 'timestamp'])
    event_processed = Event(arguments=['process_name', 'event_id', 'api_name', 'event_name', 'kwargs', 'timestamp'])

    class Meta:
        name = 'internal.metrics'
        internal = True
        auto_register = False
