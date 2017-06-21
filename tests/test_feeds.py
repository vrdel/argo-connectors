import unittest
import modules.config
from bin.topology_gocdb_connector import GOCDBReader, logger
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
        self.orig_get_xmldata = self.gocdbreader._get_xmldata
        self.gocdbreader._get_xmldata = self.wrap_get_xmldata

    def wrap_get_xmldata(self, scope, pi):
        globopts = self.globalconfig.parse()
        self.orig_get_xmldata.im_func.func_globals['globopts'] = globopts
        self.orig_get_xmldata(scope, pi)

    def testServiceEndpoints(self):
        # group_endpoints = self.gocdbreader.getGroupOfServices()
        pass

if __name__ == '__main__':
    unittest.main()
