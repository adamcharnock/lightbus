import inspect
import json as jsonlib
from pathlib import Path
from typing import Mapping, Union, Type, get_type_hints, TypeVar, Dict, NamedTuple

import jsonschema
import yaml as yamllib

from lightbus.exceptions import UnexpectedConfigurationFormat
from lightbus.schema.hints_to_schema import python_type_to_json_schemas, SCHEMA_URI

if False:
    from .structure import RootConfig, BusConfig, ApiConfig


class Config(object):
    """Provides access to configuration options

    There are two forms of configuration:

        * Bus-level configuration, `config.bus()`
        * API-level configuration, `config.api(api_name)`

    Bus-level configuration is global to lightbus. API-level configuration
    will normally have a default catch-all definition, but can be customised
    on a per-api basis.
    """
    _config: 'RootConfig'

    def __init__(self, root_config: 'RootConfig'):
        self._config = root_config

    def bus(self) -> 'BusConfig':
        return self._config.bus

    def api(self, api_name=None) -> 'ApiConfig':
        """Returns config for the given API

        If there is no API-specific config available for the
        given api_name, then the root API config will be returned.
        """
        return self._config.apis.get(api_name, None) or self._config.apis['default']

    def apis(self) -> Dict[str, 'ApiConfig']:
        return self._config.apis

    def plugin(self, plugin_name) -> NamedTuple:
        return getattr(self._config.plugins, plugin_name)

    @classmethod
    def load_file(cls, file_path):
        """Instantiate the config from the given file path

        Files ending in `.json` will be parsed as JSON, otherwise the
        file will be parsed as YAML.
        """
        file_path = Path(file_path)
        encoded_config = file_path.read_text(encoding='utf8')

        if file_path.name.endswith('.json'):
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
        config = yamllib.load(yaml)
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
        return cls(
            root_config=mapping_to_named_tuple(config, RootConfig)
        )

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
    schema['$schema'] = SCHEMA_URI
    return schema


def set_default_config(config: dict) -> dict:
    config.setdefault('apis', {})
    config.setdefault('bus', {})
    config['apis'].setdefault('default', {})
    config['bus'].setdefault('schema', {})

    config['apis']['default'].setdefault('rpc_transport', {'redis': {}})
    config['apis']['default'].setdefault('result_transport', {'redis': {}})
    config['apis']['default'].setdefault('event_transport', {'redis': {}})
    config['bus']['schema'].setdefault('transport', {'redis': {}})
    return config


T = TypeVar('T')


def mapping_to_named_tuple(mapping: Mapping, named_tuple: Type[T]) -> T:
    """Convert a dictionary-like object into the given named tuple

    This conversion is performed recursively. If the passed named tuple
    class contains child named tuples, then the the corresponding
    child keys of the dictionary will be mapped.

    This is used to take the supplied configuration and load it into the
    expected configuration structures.
    """
    import lightbus.config.structure
    hints = get_type_hints(named_tuple, None, lightbus.config.structure.__dict__)
    parameters = {}

    if mapping is None:
        return None

    for key, hint in hints.items():
        is_class = inspect.isclass(hint)
        value = mapping.get(key)

        if key not in mapping:
            continue

        # Is this an Optional[] hint (which looks like Union[Thing, None)
        subs_tree = hint._subs_tree() if hasattr(hint, '_subs_tree') else None
        if type(hint) == type(Union) and len(subs_tree) == 3 and subs_tree[2] == type(None) and value is not None:
            hint = subs_tree[1]

        if is_namedtuple(hint):
            parameters[key] = mapping_to_named_tuple(value, hint)
        elif is_class and issubclass(hint, Mapping) and subs_tree and len(subs_tree) == 3:
            parameters[key] = dict()
            for k, v in value.items():
                parameters[key][k] = mapping_to_named_tuple(v, hint._subs_tree()[2])
        else:
            parameters[key] = value

    return named_tuple(**parameters)


def is_namedtuple(v):
    """Figuring out if an object is a named tuple is not as trivial as one may expect"""
    try:
        return issubclass(v, tuple) and hasattr(v, '_fields')
    except TypeError:
        return False
