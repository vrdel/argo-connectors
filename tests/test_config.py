import unittest2 as unittest
import modules.config

class TestConfig(unittest.TestCase):
    def setUp(self):
        self.globalconfig = modules.config.Global('topology-gocdb-connector.py', 'tests/global.conf')
        self.customerconfig = modules.config.CustomerConf('topology-gocdb-connector.py', 'tests/customer.conf')
        self.globalconfig_nocaller = modules.config.Global(None, 'tests/global.conf')

    def testGlobalParse(self):
        opts = self.globalconfig.parse()
        self.assertTrue(isinstance(opts, dict))
        self.assertEqual(opts['outputtopologygroupofendpoints'], 'group_endpoints_DATE.avro')
        self.assertEqual(opts['outputtopologygroupofgroups'], 'group_groups_DATE.avro')
        self.assertEqual(opts['avroschemastopologygroupofendpoints'], 'etc/schemas//group_endpoints.avsc')
        self.assertEqual(opts['avroschemastopologygroupofgroups'], 'etc/schemas//group_groups.avsc')

        opts_nocall = self.globalconfig_nocaller.parse()
        self.assertEqual(opts_nocall['amshost'],'localhost')
        self.assertEqual(opts_nocall['authenticationverifyservercert'], 'False')
        self.assertEqual(opts_nocall['authenticationcafile'], '/etc/pki/tls/certs/ca-bundle.crt')
        self.assertEqual(opts_nocall['inputstatedays'], '3')
        self.assertEqual(opts_nocall['amstoken'], 'EGIKEY')
        self.assertEqual(opts_nocall['authenticationcapath'], '/etc/grid-security/certificates')
        self.assertEqual(opts_nocall['inputstatesavedir'], '/var/lib/argo-connectors/states/')
        self.assertEqual(opts_nocall['connectionretry'], '3')
        self.assertEqual(opts_nocall['amsproject'], 'EGI')
        self.assertEqual(opts_nocall['generalpublishams'], 'True')
        self.assertEqual(opts_nocall['generalwriteavro'], 'True')
        self.assertEqual(opts_nocall['authenticationhostcert'], '/etc/grid-security/hostcert.pem')
        self.assertEqual(opts_nocall['connectiontimeout'], '180')
        self.assertEqual(opts_nocall['authenticationhostkey'], '/etc/grid-security/hostkey.pem')
        self.assertEqual(opts_nocall['amstopic'], 'TOPIC')
        self.assertEqual(opts_nocall['amsbulk'], '100')
        self.assertEqual(opts_nocall['amspacksinglemsg'], 'True')

    def testAmsOpts(self):
        opts = self.globalconfig.parse()
        ams_incomplete = dict(amshost='host', amstoken='token')
        complete, missing = self.globalconfig.is_complete(ams_incomplete, 'ams')
        self.assertFalse(complete)
        self.assertEqual(missing, set(['amsproject', 'amstopic', 'amsbulk', 'amspacksinglemsg']))
        merged = self.globalconfig.merge_opts(ams_incomplete, 'ams')
        self.assertEqual(merged, dict(amshost='host', amsproject='EGI',
                                      amstoken='token', amstopic='TOPIC',
                                      amsbulk='100', amspacksinglemsg='True'))

    def testHttpAuthOpts(self):
        globalopts = self.globalconfig.parse()
        feeds = self.customerconfig.get_mapfeedjobs('topology-gocdb-connector.py', 'GOCDB', deffeed='https://goc.egi.eu/gocdbpi/')
        jobcust = feeds.items()[0][1]
        auth_custopts = self.customerconfig.get_authopts('foofeed', jobcust)
        auth_opts = self.globalconfig.merge_opts(auth_custopts, 'authentication')
        complete, missing = self.globalconfig.is_complete(auth_opts, 'authentication')
        self.assertEqual(complete, False)
        self.assertEqual(missing, set(['authenticationhttppass']))

    def testCustomerParse(self):
        opts = self.customerconfig.parse()
        customers = self.customerconfig.get_customers()
        self.assertEqual(customers, ['CUSTOMER_EGI'])
        jobs = self.customerconfig.get_jobs(customers[0])
        self.assertEqual(jobs, ['JOB_EGICritical', 'JOB_EGIFedcloud'])
        custdir = self.customerconfig.get_custdir(customers[0])
        self.assertEqual(custdir, '/var/lib/argo-connectors/EGI/')
        ggtags = self.customerconfig.get_gocdb_ggtags(jobs[0])
        self.assertEqual(ggtags, {'NGI': 'EGI.eu', 'Infrastructure':
                                  'Production', 'Certification': 'Certified',
                                  'Scope': 'EGI'})
        getags = self.customerconfig.get_gocdb_getags(jobs[0])
        self.assertEqual(getags, {'Scope': 'EGI', 'Production': 'Y', 'Monitored': 'Y'})
        profiles = self.customerconfig.get_profiles(jobs[0])
        self.assertEqual(profiles, ['ARGO_MON_CRITICAL'])
        feedjobs = self.customerconfig.get_mapfeedjobs('topology-gocdb-connector.py',
                                                       'GOCDB',
                                                       deffeed='https://goc.egi.eu/gocdbpi/')
        self.assertEqual(feedjobs, {'https://goc.egi.eu/gocdbpi/':
                                    [('JOB_EGICritical', 'CUSTOMER_EGI'),
                                     ('JOB_EGIFedcloud', 'CUSTOMER_EGI')]})

    def testPoemServerOpts(self):
        customerconfig = modules.config.CustomerConf('poem-connector.py', 'tests/customer.conf')
        opts = customerconfig.parse()
        customers = self.customerconfig.get_customers()
        jobs = self.customerconfig.get_jobs(customers[0])
        namespace = self.customerconfig.get_namespace(jobs[0])
        self.assertEqual(namespace, 'ch.cern.SAM')
        serverhost = self.customerconfig.get_poemserver_host(jobs[0])
        self.assertEqual(serverhost, 'poem-devel.argo.grnet.gr')
        vo = self.customerconfig.get_poemserver_vo(jobs[0])
        self.assertEqual(vo, ['ops'])


if __name__ == '__main__':
    unittest.main()
