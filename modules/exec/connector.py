import argparse
import os

from argo_connectors.log import Logger
from argo_connectors.utils import date_check


class ExecConnector:
    def __init__(self, description, task, initial_arg=False,
                 initial_arg_help='', exe_script=''):
        self.description = description
        self.task = task
        self.initial_arg = initial_arg
        self.initial_arg_help = initial_arg_help
        self.exe_script = exe_script
        self.args = None
        self.fixed_date = None
        self.confpath = None
        self.__main()

    def _setargs(self):
        parser = argparse.ArgumentParser(description=self.description)
        parser.add_argument('-c', dest='custconf', metavar='customer.conf',
                            default=None, help='path to customer configuration file',
                            type=str, required=False)
        if self.initial_arg:
            parser.add_argument('--initial', dest='initsync',
                                help=self.initial_arg_help,
                                action='store_true', default=False,
                                required=False)
        parser.add_argument('-g', dest='gloconf', metavar='global.conf',
                            default=None, help='path to global configuration file',
                            type=str, required=False)
        parser.add_argument('-d', dest='date', metavar='YEAR-MONTH-DAY',
                            help='write data for this date', type=str, required=False)
        self.args = args = parser.parse_args()

        if args.date and date_check(args.date):
            self.fixed_date = args.date

        if args.gloconf:
            self.confpath = args.gloconf

    def _main(self):
        self._setargs()
        self.logger = Logger(os.path.basename(self.exe_script))
