# `lightbus dumpconfigschema`

This command will output a [JSON schema](https://json-schema.org/) for the 
global bus configuration file.

The global bus configuration file is typically written as YAML, but it can also be written as JSON.
In which case, you can validate the structure against the JSON schema produced by this 
command.

This schema can also be loaded into some editors to provide auto-completion when 
editing your bus' configuration file.

!!! important

    Be careful not to confuse this command with `dumpschema`. The `dumpschema` 
    command dumps your bus' schema, whereas this `dumpconfigschema` simply 
    dumps the schema for your bus' configuration.
    

## Examples

Dump the configuration file schema to standard out:

```
lightbus dumpschema
```

Dump the configuration file schema to a file:

```
lightbus dumpschema --out my_schema.json
Schema for 3 APIs saved to my_schema.json
```

## Option reference

```
$ lightbus dumpconfigschema --help
usage: lightbus dumpconfigschema [-h] [--out FILE] [--bus BUS_MODULE]
                                 [--service-name SERVICE_NAME]
                                 [--process-name PROCESS_NAME] [--config FILE]
                                 [--log-level LOG_LEVEL]

optional arguments:
  -h, --help            show this help message and exit

Dump config schema command arguments:
  --out FILE, -o FILE   File to write config schema to. If omitted the schema
                        will be written to standard out. (default: None)

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
