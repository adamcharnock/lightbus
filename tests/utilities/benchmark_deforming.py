from copy import copy

import pytest

from lightbus.utilities.deforming import deform_to_bus
from tests.utilities.test_unit_deforming import DEFORMATION_TEST_PARAMETERS

pytestmark = pytest.mark.benchmark


@pytest.mark.parametrize(
    "test_input,expected",
    list(DEFORMATION_TEST_PARAMETERS.values()),
    ids=list(DEFORMATION_TEST_PARAMETERS.keys()),
)
@pytest.mark.benchmark(group="deforming")
def benchmark_deform_to_bus(test_input, expected, benchmark):
    benchmark(deform_to_bus, test_input)
