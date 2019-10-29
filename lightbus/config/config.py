import json as jsonlib
import os
from pathlib import Path
from typing import Dict, NamedTuple, Union, Optional
import urllib.request

import jsonschema
import yaml as yamllib

from lightbus.exceptions import UnexpectedConfigurationFormat
from lightbus.schema.hints_to_schema import python_type_to_json_schemas, SCHEMA_URI
from lightbus.utilities.casting import cast_to_hint
from lightbus.utilities.deforming import deform_to_bus

if False:
    # pylint: disable=unused-import
    from .structure import RootConfig, BusConfig, ApiConfig, ConfigProxy


class Config(object):
    """Provides access to configuration options

    There are two forms of configuration:

        * Bus-level configuration, `config.bus()`
        * API-level configuration, `config.api(api_name)`

    Bus-level configuration is global to lightbus. API-level configuration
    will normally have a default catch-all definition, but can be customised
    on a per-api basis.
    """

    _config: "RootConfig"

    def __init__(self, root_config: "RootConfig", source: Optional[dict] = None):
        self._config = root_config
        if source is not None:
            self._source = source
        else:
            # Use the default config as a source if none is specified.
            # This is triggered when instantiating a bus client
            # with a RootConfig object rather than loading in a dict or json.
            # This frequently happens in testing.
            self._source = Config.load_dict({})._source

    def bus(self) -> "BusConfig":
        return self._config.bus

    def api(self, api_name=None) -> Union["ApiConfig", "ConfigProxy"]:
        """Returns config for the given API

        If there is no API-specific config available for the
        given api_name, then the root API config will be returned.
        """
        from .structure import ApiConfig, ConfigProxy

        return ConfigProxy(
            (self._config.apis.get(api_name, None), self._source.get("apis", {}).get(api_name, {})),
            (
                self._config.apis.get("default", None),
                self._source.get("apis", {}).get("default", {}),
            ),
            (ApiConfig(), {}),
        )

    def apis(self) -> Dict[str, "ApiConfig"]:
        return self._config.apis

    def plugin(self, plugin_name) -> NamedTuple:
        return getattr(self._config.plugins, plugin_name)

    @classmethod
    def load_file(cls, file_path: Union[str, Path]):
        """Instantiate the config from the given file path

        Files ending in `.json` will be parsed as JSON, otherwise the
        file will be parsed as YAML.
        """

        if str(file_path).startswith("http://") or str(file_path).startswith("https://"):
            response = urllib.request.urlopen(file_path, timeout=5)
            encoded_config = response.read()
            if "json" in response.headers.get("Content-Type") or file_path.endswith(".json"):
                return cls.load_json(encoded_config)
            else:
                return cls.load_yaml(encoded_config)
        else:
            file_path = Path(file_path)
            encoded_config = file_path.read_text(encoding="utf8")

            if file_path.name.endswith(".json"):
                return cls.load_json(encoded_config)
            else:
                return cls.load_yaml(encoded_config)

    @classmethod
    def load_json(cls, json: str):
        """Instantiate the config from a JSON string"""
        return cls.load_dict(config=jsonlib.loads(json))

    @classmethod
    def load_yaml(cls, yaml: str):
        """Instantiate the config from a YAML string"""
        config = yamllib.safe_load(yaml)
        if not isinstance(config, dict):
            raise UnexpectedConfigurationFormat(
                f"The config file was loaded but it appears to be in an unexpected format. "
                f"The root of the configuration should be a key/value mapping, but the "
                f"type '{type(config).__name__}' was found instead. Check your config "
                f"file is correctly formatted."
            )
        return cls.load_dict(config=config)

    @classmethod
    def load_dict(cls, config: dict, set_defaults=True):
        """Instantiate the config from a dictionary"""
        from .structure import RootConfig

        config = config.copy()
        if set_defaults:
            config = set_default_config(config)
        validate_config(config)

        root_config = cast_to_hint(config, RootConfig)
        return cls(root_config, source=config)

    def __getattr__(self, item):
        if hasattr(self._config, item):
            return getattr(self._config, item)
        else:
            raise AttributeError(f"No root-level configuration option named '{item}'")


def validate_config(config: dict):
    """Validate the provided config dictionary against the config json schema"""
    json_schema = config_as_json_schema()
    jsonschema.validate(config, json_schema)


def config_as_json_schema() -> dict:
    """Get the configuration structure as a json schema"""
    from .structure import RootConfig

    schema, = python_type_to_json_schemas(RootConfig)
    # Some of the default values will still be python types,
    # so let's use deform_to_bus to turn them into something
    # that'll be json safe
    schema = deform_to_bus(schema)

    schema["$schema"] = SCHEMA_URI
    return schema


def set_default_config(config: dict) -> dict:
    """Set the default configuration options on a loaded config dictionary"""
    env_service_name = os.environ.get("LIGHTBUS_SERVICE_NAME")
    if env_service_name:
        config.setdefault("service_name", env_service_name)

    env_process_name = os.environ.get("LIGHTBUS_PROCESS_NAME")
    if env_process_name:
        config.setdefault("process_name", env_process_name)

    config.setdefault("apis", {})
    config.setdefault("bus", {})
    config["apis"].setdefault("default", {})
    config["bus"].setdefault("schema", {})

    config["apis"]["default"].setdefault("rpc_transport", {"redis": {}})
    config["apis"]["default"].setdefault("result_transport", {"redis": {}})
    config["apis"]["default"].setdefault("event_transport", {"redis": {}})
    config["bus"]["schema"].setdefault("transport", {"redis": {}})
    return config
