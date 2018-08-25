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

## Community

TODO - Own section?

Quote regarding Rust:

> For newbs/outsiders looking in, I think a lot of it has to do with a culture of validating people's experiences.
> That's difficult. If a user complains about it, the natural reaction can easily be to
> go defensive ("You're doing it wrong" or "we documented that here"). Rust has successfully
> fostered a culture of approaching it as an opportunity to learn something ("Why did you not see the documentation").

We should do that.

Everyone is coming from different cultures with different language skills.
Assume perceived rudeness is due to different social norms or
poorer English skills. Perhaps assume the person has 3 children
running around screaming while they filed the bug report. All these
attributes stack the deck against them contributing, so try to be
especially kind, patient, and grateful, even though your
initial response may be the opposite.
