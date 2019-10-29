# `lightbus run`

The `lightbus run` command is used to **start the Lightbus worker process** and will be 
used in any Lightbus deployment. See the [anatomy lesson] for further details.

## Examples

In its **basic form** `lightbus run` will expect to be able to import a module 
called `bus` which will contain your bus client:

```
lightbus run
```

---

You can also **enable/disable specific features**. For example, you may 
wish to run a worker which responds soley to RPCs in order to ensure a 
timely response:

```
# Handles RPCs
lightbus run --only rpcs

# Handles everything else (events and tasks)
lightbus run --skip rpcs
```

---

**A production use** may look something like this 
(for more information on service & process names see the [events explanation]):

```
lightbus run \
    --bus my_company.my_project.bus \
    --service-name video-encoder \
    --process-name lightbus-worker-1 \
    --config /etc/lightbus/global-bus.yaml \
    --log-level debug
```

This could also be re-written as:

```shell
export LIGHTBUS_MODULE=my_company.my_project.bus
export LIGHTBUS_SERVICE_NAME=video-encoder
export LIGHTBUS_PROCESS_NAME=lightbus-worker-1

lightbus run \
    --config /etc/lightbus/global-bus.yaml \
    --log-level debug
```

---

**Configuration can also be loaded over HTTP or HTTPS**. This may be useful if you 
wish to pull the global bus config from an (internal-only) endpoint/service:

```
# Can load JSON/YAML over HTTP/HTTPS
lightbus run --config http://config.internal/lightbus/global-bus.yaml
```

## Option reference

```
$ lightbus run --help
usage: lightbus run [-h] [--only ONLY] [--skip SKIP]
                    [--schema FILE_OR_DIRECTORY] [--bus BUS_MODULE]
                    [--service-name SERVICE_NAME]
                    [--process-name PROCESS_NAME] [--config FILE]
                    [--log-level LOG_LEVEL]

optional arguments:
  -h, --help            show this help message and exit

Run command arguments:
  --only ONLY, -o ONLY  Only provide the specified features. Comma separated
                        list. Possible values: rpcs, events, tasks (default:
                        None)
  --skip SKIP, -k SKIP  Provide all except the specified features. Comma
                        separated list. Possible values: rpcs, events, tasks
                        (default: None)
  --schema FILE_OR_DIRECTORY, -m FILE_OR_DIRECTORY
                        Manually load the schema from the given file or
                        directory. This will normally be provided by the
                        schema transport, but manual loading may be useful
                        during development or testing. (default: None)

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


[anatomy lesson]: ../../explanation/anatomy-lesson.md
[events explanation]: ../../explanation/events.md#service-names-listener-names
