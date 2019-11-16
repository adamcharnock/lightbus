"""Data structures used for specifying which features a lightbus process should provide"""
from enum import Enum


class Feature(Enum):
    RPCS = "rpcs"
    EVENTS = "events"
    TASKS = "tasks"


ALL_FEATURES = tuple(Feature)
