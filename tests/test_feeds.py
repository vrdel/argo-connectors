import unittest
import modules.config
from httmock import urlmatch, HTTMock, response
from bin.topology_gocdb_connector import GOCDBReader, logger, globopts
from modules import input
from modules.helpers import module_class_name

class TopologyFeed(unittest.TestCase):
    def setUp(self):
        self.globalconfig = modules.config.Global('topology-gocdb-connector.py', 'tests/global.conf')
        self.customerconfig = modules.config.CustomerConf('topology-gocdb-connector.py', 'tests/customer.conf')
        feedjobs = self.customerconfig.get_mapfeedjobs('topology-gocdb-connector.py',
                                                       'GOCDB',
                                                       deffeed='https://localhost/gocdbpi/')
        feed = feedjobs.keys()[0]
        jobcust = feedjobs.values()[0]
        scopes = self.customerconfig.get_feedscopes(feed, jobcust)
        self.gocdbreader = GOCDBReader(feed, scopes)
        self.gocdbreader._get_xmldata = self.overriden_get_xmldata

    def overriden_get_xmldata(self, scope, pi):
        globopts = self.globalconfig.parse()
        self._o = self.gocdbreader._o
        res = input.connection(logger, globopts, self._o.scheme, self._o.netloc,
                                pi + scope,
                                module_class_name(self))
        doc = input.parse_xml(logger, res, self._o.scheme + '://' + self._o.netloc + pi,
                        module_class_name(self))
        return doc

    def testServiceEndpoints(self):
        # group_endpoints = self.gocdbreader.getGroupOfServices()
        pass

if __name__ == '__main__':
    unittest.main()
