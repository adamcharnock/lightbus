import json

import asyncpg
import pytest

from lightbus import EventMessage
from lightbus.exceptions import UnsupportedOptionValue
from lightbus.transports.transactional import AsyncPostgresConnection
from tests.transactional_transport.conftest import verification_connection
