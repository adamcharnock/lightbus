TBA. Contributions welcome!

## Development

Put `asyncio` into debug mode:

    PYTHONASYNCIODEBUG=1

## Running the tests

First you will need to install the testing requirements:

    pip install .[development]

You will also need Redis 5 or above available locally, specify the path
to the `redis-server` command using the `REDIS_SERVER` environment
variable:

    REDIS_SERVER=/Users/adam/Projects/redis/src/redis-server

Now you can run the tests using:

    pytest

Note that you can run subsets of the tests as follows:

    pytest -m unit  # Fast with high coverage
    pytest -m integration
    pytest -m reliability
