"""Plugin to broadcast Lightbus' state on the internal.state API"""
import asyncio

from datetime import datetime

from lightbus import BusClient
from lightbus.api import registry
from lightbus.message import EventMessage
from lightbus.plugins import LightbusPlugin, is_plugin_loaded
from lightbus.utilities import handle_aio_exceptions, block


class MetricsPlugin(LightbusPlugin):
    priority = 110
