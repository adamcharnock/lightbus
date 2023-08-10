import inspect
import pytest

import lightbus
import lightbus.path
from lightbus.transports.redis.event import StreamUse

pytestmark = pytest.mark.integration

stream_use_test_data = [StreamUse.PER_EVENT, StreamUse.PER_API]

pytestmark = pytest.mark.benchmark


class BenchmarkApi(lightbus.Api):
    fire_me = lightbus.Event()

    class Meta:
        name = "benchmark"

    def call_me(self):
        return True


BUS_MODULE_CONTENT = f"""
bus = lightbus.create(plugins=[])

{inspect.getsource(BenchmarkApi)}

bus.client.register_api(BenchmarkApi())
"""


@pytest.fixture()
def run_lightbus(run_lightbus_command, make_test_bus_module):
    """Run lightbus in a background process"""
    run_lightbus_command(
        "run", "--bus", make_test_bus_module(code=BUS_MODULE_CONTENT), env={"LIGHTBUS_MODULE": ""}
    )


@pytest.fixture()
def bus(redis_config_file):
    """Get a BusPath instance so we can use the bus"""
    bus = lightbus.create(config_file=redis_config_file)
    yield bus
    bus.client.close()


@pytest.mark.benchmark(group="network")
def benchmark_call_rpc(run_lightbus, bus, benchmark):
    def benchmark_me():
        assert bus.benchmark.call_me()

    benchmark.pedantic(benchmark_me, rounds=20, warmup_rounds=1)


@pytest.mark.benchmark(group="network")
def benchmark_fire_event(bus, benchmark):
    bus.client.register_api(BenchmarkApi())
    benchmark.pedantic(bus.benchmark.fire_me.fire, rounds=20, warmup_rounds=1)
