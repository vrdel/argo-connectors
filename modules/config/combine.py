import yaml
import os

from argo_connectors.log import Logger


class CombineConf(object):
    def __init__(self, caller, confpath, **kwargs):
        self.logger = Logger(str(self.__class__))
        self.confpath = confpath

    def parse(self):
        if not os.path.exists(self.confpath):
            self.logger.error('Could not find %s' % self._filename)
            raise SystemExit(1)

        with open(self.confpath, 'r') as file:
            data = yaml.safe_load(file)

        import ipdb; ipdb.set_trace()

