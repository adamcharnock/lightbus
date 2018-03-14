import logging

from lightbus.log import LightbusFormatter

logger = logging.getLogger(__name__)


def configure_logging():
    import logging

    logger = logging.getLogger('lightbus')
    handler = logging.StreamHandler()

    formatter = LightbusFormatter(fmt={
        'DEBUG': '%(log_color)s%(name)s | %(msg)s',
        'INFO': '%(log_color)s%(name)s | %(msg)s',
        'WARNING': '%(log_color)s%(name)s | %(msg)s',
        'ERROR': '%(log_color)s%(name)s | ERROR: %(msg)s (%(module)s:%(lineno)d)',
        'CRITICAL': '%(log_color)s%(name)s | CRITICAL: %(msg)s',
    })
    handler.setFormatter(formatter)

    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)  # config: log_level
