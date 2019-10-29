# `lightbus shell`

The `lightbus shell` command provides an interactive prompt through which 
you can interface with the bus.

To use this command you must first install `bpython`:

```
pip install bpython
```

## Examples

You should see the following when starting up the shell. This is a fully functional 
Python shell, with your bus loaded in a ready to be used:

``` 
$ lightbus shell
>>> █ 
Welcome to the Lightbus shell. Use `bus` to access your bus.\
```

Upon typing the shell will begin to auto-complete based on the locally available APIs:

```
$ lightbus shell
>>> bus.au█
┌──────────────────────────────────────────────────────────────────────────┐
│ auth                                                                     │
└──────────────────────────────────────────────────────────────────────────┘
```

You can fire events and call RPCs as follows:

```
# Fire an event
>>> bus.auth.user_registered.fire(email="joe@example.com", username="joe")

# Call an RPC
>>> bus.auth.check_password(username="admin", password="secret")
True
```

## Option reference

```
$ lightbus shell --help
usage: lightbus shell [-h] [--bus BUS_MODULE] [--service-name SERVICE_NAME]
                      [--process-name PROCESS_NAME] [--config FILE]
                      [--log-level LOG_LEVEL]

optional arguments:
  -h, --help            show this help message and exit

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
