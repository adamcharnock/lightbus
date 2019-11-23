# `lightbus dumpschema`

The `lightbus dumpschema` command will dump the bus' JSON schema to either a file or 
standard out.

This schema file can then be manually provided to `lightbus run` using the 
`--schema` option.

## Why is this useful?

The idea behind this command is to aid in testing and local development. 
You can take a dump of your production bus' schema and use it in your 
local development or testing environment.

This will allow Lightbus to validate your locally emitted events and RPCs 
against the expectations of your production environment.

See [manual validation](../schema.md#manual-validation) for more information.

## Examples

Dump the schema to standard out:

```
lightbus dumpschema
```

Dump the schema to a file:

```
lightbus dumpschema --out my_schema.json
Schema for 3 APIs saved to my_schema.json
```

## Options reference

```
$ lightbus dumpschema --help
usage: lightbus dumpschema [-h] [--out FILE_OR_DIRECTORY] [--bus BUS_MODULE]
                           [--service-name SERVICE_NAME]
                           [--process-name PROCESS_NAME] [--config FILE]
                           [--log-level LOG_LEVEL]

optional arguments:
  -h, --help            show this help message and exit

Dump config schema command arguments:
  --out FILE_OR_DIRECTORY, -o FILE_OR_DIRECTORY
                        File or directory to write schema to. If a directory
                        is specified one schema file will be created for each
                        API. If omitted the schema will be written to standard
                        out. (default: None)

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
