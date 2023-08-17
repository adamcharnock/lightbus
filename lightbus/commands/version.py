import argparse
import logging
import sys
from pathlib import Path

from importlib.metadata import version as importlib_version

if sys.version_info < (3, 10):
    from importlib.metadata import entry_points as _entry_points

    def entry_points(group):
        return _entry_points()[group]

else:
    from importlib.metadata import entry_points


logger = logging.getLogger(__name__)


class Command:
    def setup(self, parser, subparsers):
        parser_version = subparsers.add_parser(
            "version", help="Show the currently installed Lightbus version"
        )
        # Read version directly out of pyproject.toml. Useful for the release process
        parser_version.add_argument("--pyproject", action="store_true", help=argparse.SUPPRESS)
        # Show the version to be used for creating the docs
        parser_version.add_argument("--docs", action="store_true", help=argparse.SUPPRESS)
        parser_version.set_defaults(func=self.handle)

    def handle(self, args):
        if args.pyproject:
            import lightbus
            import toml

            file_path = Path(lightbus.__file__).parent.parent / "pyproject.toml"
            with file_path.open() as f:
                version = toml.load(f)["tool"]["poetry"]["version"]
            if args.docs:
                version = ".".join(version.split(".")[:2])
            print(version)
        else:
            print(importlib_version("lightbus"))
