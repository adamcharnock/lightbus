import logging

from lightbus.log import LightbusFormatter

handler = logging.StreamHandler()


def configure_logging(log_level=logging.INFO):
    logger = logging.getLogger("lightbus")
    logger.propagate = False

    formatter = LightbusFormatter()
    handler.setFormatter(formatter)

    logger.removeHandler(handler)
    logger.addHandler(handler)
    logger.setLevel(log_level)
