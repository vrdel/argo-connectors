from argo_egi_connectors.helpers import module_class_name
from argo_egi_connectors.log import Logger

class VaporParse(object):
    def __init__(self, logger, data):
        self.data = data
        self.logger = logger

    def get_data(self):
        try:
            weights = dict()
            for ngi in self.data:
                for site in ngi['site']:
                    key = site['id']
                    if 'ComputationPower' in site:
                        val = site['ComputationPower']
                    else:
                        self.logger.warn(module_class_name(self) + ': No ComputationPower value for NGI:%s Site:%s' % (ngi['ngi'], site['id']))
                        val = '0'
                    weights[key] = val

            return weights

        except (KeyError, IndexError) as e:
            self.logger.error(module_class_name(self) + ': Error parsing feed - %s' % (repr(e).replace('\'', '')))
            return False
