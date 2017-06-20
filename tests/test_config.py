import unittest
import modules.config

class TestConfig(unittest.TestCase):
    def setUp(self):
        self.globalconfig = modules.config.Global('topology-gocdb-connector.py', 'tests/global.conf')
        self.customerconfig = modules.config.CustomerConf('topology-gocdb-connector.py', 'tests/customer.conf')

    def testGlobalParse(self):
        opts = self.globalconfig.parse()
        self.assertTrue(isinstance(opts, dict))
        self.assertEqual(opts['outputtopologygroupofendpoints'], 'group_endpoints_DATE.avro')
        self.assertEqual(opts['outputtopologygroupofgroups'], 'group_groups_DATE.avro')
        self.assertEqual(opts['avroschemastopologygroupofendpoints'], '/etc/argo-egi-connectors/schemas//group_endpoints.avsc')
        self.assertEqual(opts['avroschemastopologygroupofgroups'], '/etc/argo-egi-connectors/schemas//group_groups.avsc')

    def testAmsOpts(self):
        opts = self.globalconfig.parse()
        ams_incomplete = dict(amshost='host', amstoken='token')
        complete, missing = self.globalconfig.is_complete(ams_incomplete, 'ams')
        self.assertFalse(complete)
        self.assertEqual(missing, set(['amsproject', 'amstopic', 'amsbulk']))
        merged = self.globalconfig.merge_opts(ams_incomplete, 'ams')
        self.assertEqual(merged, dict(amshost='host', amsproject='EGI', amstoken='token', amstopic='TOPIC'))

    def testCustomerParse(self):
        opts = self.customerconfig.parse()
        customers = self.customerconfig.get_customers()
        self.assertEqual(customers, ['CUSTOMER_EGI'])
        jobs = self.customerconfig.get_jobs(customers[0])
        self.assertEqual(jobs, ['JOB_EGICritical', 'JOB_EGICloudmon'])
        custdir = self.customerconfig.get_custdir(customers[0])
        self.assertEqual(custdir, '/var/lib/argo-connectors/EGI/')
        ggtags = self.customerconfig.get_gocdb_ggtags(jobs[0])
        self.assertEqual(ggtags, {'Infrastructure': 'Production', 'Certification': 'Certified', 'Scope': 'EGI'})
        getags = self.customerconfig.get_gocdb_getags(jobs[0])
        self.assertEqual(getags, {'Scope': 'EGI', 'Production': 'Y', 'Monitored': 'Y'})
        profiles = self.customerconfig.get_profiles(jobs[0])
        self.assertEqual(profiles, ['ROC_CRITICAL'])
        feedjobs = self.customerconfig.get_mapfeedjobs('topology-gocdb-connector.py',
                                                       'GOCDB',
                                                       deffeed='https://goc.egi.eu/gocdbpi/')
        self.assertEqual(feedjobs, {'https://goc.egi.eu/gocdbpi/':
                                    [('JOB_EGICritical', 'CUSTOMER_EGI'),
                                     ('JOB_EGICloudmon', 'CUSTOMER_EGI')]})



if __name__ == '__main__':
    unittest.main()
