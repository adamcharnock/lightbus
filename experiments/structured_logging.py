""" Experimenting with using structlog with hope of supporting it in Lightbus
"""
import logging
import sys

import structlog


def event_dict_ordering(logger, method_name, event_dict):
    ordered = {"event": event_dict.pop("event")}
    ordered.update(**event_dict)
    return ordered


structlog.configure(
    processors=[
        event_dict_ordering,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.dev.ConsoleRenderer()
        if sys.stdout.isatty()
        else structlog.processors.JSONRenderer(),
    ]
)


if __name__ == "__main__":
    log = structlog.wrap_logger(logging.getLogger("test"))
    log.warning("hello from std", foo=1)

    log.info("Loaded plugins", plugins={...}, context={"service_name": "..."})
