import inspect
import json as jsonlib
from pathlib import Path
from typing import Mapping, Union, Type, NamedTuple, get_type_hints, TypeVar, Callable

import yaml as yamllib

from lightbus.schema.hints_to_schema import python_type_to_json_schemas, SCHEMA_URI
from .structure import RootConfig, BusConfig, ApiConfig


class Config(object):
    _config: RootConfig

    def __init__(self, root_config: RootConfig):
        self._config = root_config

    def bus(self) -> BusConfig:
        return self._config.bus

    def api(self, api_name=None) -> ApiConfig:
        """Returns config for the given API

        If there is no API-specific config available for the
        given api_name, then the root API config will be returned.
        """
        return self._config.apis.get(api_name, None) or self._config.apis['default']

    @classmethod
    def load_file(cls, file_path):
        file_path = Path(file_path)
        encoded_config = file_path.read_text(encoding='utf8')

        if file_path.name.endswith('.json'):
            return cls.load_json(encoded_config)
        else:
            return cls.load_yaml(encoded_config)

    @classmethod
    def load_json(cls, json: str):
        return cls.load_mapping(mapping=jsonlib.loads(json))

    @classmethod
    def load_yaml(cls, yaml: str):
        return cls.load_mapping(mapping=yamllib.load(yaml))

    @classmethod
    def load_mapping(cls, mapping: Mapping):

        return cls(
            root_config=mapping_to_named_tuple(mapping, RootConfig)
        )


def config_as_json_schema():
    schema, = python_type_to_json_schemas(RootConfig)
    schema['$schema'] = SCHEMA_URI
    return schema


T = TypeVar('T')


def mapping_to_named_tuple(mapping: Mapping, named_tuple: Type[T]) -> T:
    import lightbus.config.structure
    hints = get_type_hints(named_tuple, None, lightbus.config.structure.__dict__)
    parameters = {}

    if mapping is None:
        return None

    for key, hint in hints.items():
        value = mapping.get(key)
        if value is None:
            parameters[key] = None
        elif is_namedtuple(hint):
            parameters[key] = mapping_to_named_tuple(value, hint)
        elif inspect.isclass(hint) and issubclass(hint, Mapping) and hasattr(hint, '_subs_tree') and is_namedtuple(hint._subs_tree()[2]):
            parameters[key] = dict()
            for k, v in value.items():
                parameters[key][k] = mapping_to_named_tuple(v, hint._subs_tree()[2])
        else:
            parameters[key] = value
    return named_tuple(**parameters)


def is_namedtuple(v):
    try:
        return issubclass(v, tuple) and hasattr(v, '_fields')
    except TypeError:
        return False
