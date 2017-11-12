import lightbus
import lightbus.transports.debug
from lightbus.utilities import configure_logging


def main():
    configure_logging()

    bus = lightbus.create()

    bus.run_forever()


if __name__ == '__main__':
    main()
