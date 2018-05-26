import logging

logger = logging.getLogger(__name__)


def make_file_safe_api_name(api_name):
    """Make an api name safe for use in a file name"""
    return "".join([c for c in api_name if c.isalpha() or c.isdigit() or c in (".", "_", "-")])
