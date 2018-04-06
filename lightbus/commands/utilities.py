import importlib.util
import logging

from lightbus.utilities.autodiscovery import autodiscover

logger = logging.getLogger(__name__)


class BusImportMixin(object):

    def setup_import_parameter(self, argument_group):
        group = argument_group.add_mutually_exclusive_group()
        group.add_argument('--import', '-i',
                           dest='imprt',
                           metavar='PYTHON_MODULE',
                           help='The Python module to import initially. Will autodetect if omitted. '
                                'Cannot specify both this option and --bus.')
        group.add_argument('--bus', '-b',
                           dest='bus_path',
                           metavar='BUS_PATH',
                           help='The path to the bus.py file. Will autodetect if omitted. '
                                'Cannot specify both this option an --import.')

    def import_bus(self, args):
        if args.imprt:
            spec = importlib.util.find_spec(args.imprt)
            if not spec:
                logger.critical("Could not find module '{}' as specified by --import. Ensure "
                                "this module is available on your PYTHONPATH.".format(args.imprt))
                return
            bus_module = importlib.util.module_from_spec(spec)
        else:
            bus_module = autodiscover(bus_path=args.bus_path)

        if bus_module is None:
            logger.warning('Could not find a bus.py file, will listen for events only.')

        return bus_module
