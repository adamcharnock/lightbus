# `lightbus inspect`

The `lightbus inspect` command allows for inspecting activity on the bus.
This is currently limited to debugging only events, not RPCs.

The querying facilities provided here are not very performant. The 
`lightbus inspect` command will pull a large number of messages 
from the bus and apply filters locally.

!!! important

    `lightbus inspect` will only return results for APIs which are 
    currently being served by worker processes. If you have no workers running 
    you will see no results. This is because `lightbus inspect` relies on 
    [the schema](../../explanation/schema.md) being present on the bus.

## Examples

You can query the bus for a **specific lightbus event ID**:

```
# Will produce JSON output
lightbus inspect --id 1bff06d0-fa9b-11e9-b8ca-f218986bf8ce

# Will produce human-readable output
lightbus inspect --id 1bff06d0-fa9b-11e9-b8ca-f218986bf8ce --format human
```

---

You can also query the bus using the message ID native to the underlying broker
(in this case, this will be the Redis streams message ID):

```
lightbus inspect --native-id 1572389392059-0
```

---

You can also query by **API and/or event name**:

```
# Event/api filtering
lightbus inspect --event user_registered 
lightbus inspect --api my_company.auth 
```

---

**Wildcards** are also supported in event & api filtering:

```
# Wildcard filtering
lightbus inspect --event user_* 
lightbus inspect --api my_company.*
```

!!! note "Unexpected results when using wildcards?"

    You may need to quote wilcard strings in order to prevent your shell expanding them.
    For example:
    
        lightbus inspect --api "my_company.*"

---

You can see a **continually updating stream** of all events for a single API:

```
# Continually show events for the given API 
lightbus inspect --follow --api auth
```

---

You can query based on the **data sent in the event**. For example, to query based on the 
value of the `email` parameter of a fired event:

```
lightbus inspect --json-path email=joe@example.com
```

You can also query on more complex **[JSON path](https://goessner.net/articles/JsonPath/) expressions**:

```
lightbus inspect --json-path address.city=London
```

## Cache

This command will maintain an cache of fetched messages in `~/.lightbus/`. This will 
speed up subsequent executions and reduce the load on the bus.

## Option reference

```
$ lightbus inspect --help
usage: lightbus inspect [-h] [--json-path JSON_SEARCH]
                        [--id LIGHTBUS_EVENT_ID] [--native-id NATIVE_EVENT_ID]
                        [--api API_NAME] [--event EVENT_NAME]
                        [--version VERSION_NUMBER] [--format FORMAT]
                        [--cache-only] [--follow] [--internal]
                        [--bus BUS_MODULE] [--service-name SERVICE_NAME]
                        [--process-name PROCESS_NAME] [--config FILE]
                        [--log-level LOG_LEVEL]

optional arguments:
  -h, --help            show this help message and exit

Inspect command arguments:
  --json-path JSON_SEARCH, -j JSON_SEARCH
                        Search event body json for the givn value. Eg.
                        address.city=London (default: None)
  --id LIGHTBUS_EVENT_ID, -i LIGHTBUS_EVENT_ID
                        Find a single event with this Lightbus event ID
                        (default: None)
  --native-id NATIVE_EVENT_ID, -n NATIVE_EVENT_ID
                        Find a single event with this broker-native ID
                        (default: None)
  --api API_NAME, -a API_NAME
                        Find events for this API name. Supports the '*'
                        wildcard. (default: None)
  --event EVENT_NAME, -e EVENT_NAME
                        Find events for this event name. Supports the '*'
                        wildcard. (default: None)
  --version VERSION_NUMBER, -v VERSION_NUMBER
                        Find events with the specified version number. Can be
                        prefixed by <, >, <=, >= (default: None)
  --format FORMAT, -F FORMAT
                        Formatting style. One of json, pretty, or human.
                        (default: json)
  --cache-only, -c      Search the local cache only (default: False)
  --follow, -f          Continually listen for new events matching the search
                        criteria. May only be used on a single API (default:
                        False)
  --internal, -I        Include internal APIs (default: False)

Common arguments:
  --bus BUS_MODULE, -b BUS_MODULE
                        The bus module to import. Example 'bus',
                        'my_project.bus'. Defaults to the value of the
                        LIGHTBUS_MODULE environment variable, or 'bus'
                        (default: None)
  --service-name SERVICE_NAME, -s SERVICE_NAME
                        Name of service in which this process resides. YOU
                        SHOULD LIKELY SET THIS IN PRODUCTION. Can also be set
                        using the LIGHTBUS_SERVICE_NAME environment. Will
                        default to a random string. (default: None)
  --process-name PROCESS_NAME, -p PROCESS_NAME
                        A unique name of this process within the service. Can
                        also be set using the LIGHTBUS_PROCESS_NAME
                        environment. Will default to a random string.
                        (default: None)
  --config FILE         Config file to load, JSON or YAML (default: None)
  --log-level LOG_LEVEL
                        Set the log level. Overrides any value set in config.
                        One of debug, info, warning, critical, exception.
                        (default: None)
```
