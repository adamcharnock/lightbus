import argparse
import logging
from pathlib import Path

import pkg_resources


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
            print(pkg_resources.get_distribution("lightbus").version)
