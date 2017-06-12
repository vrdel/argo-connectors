import unittest
import modules.config

class TestGlobalConfig(unittest.TestCase):
    def setUp(self):
        self.globalconfig = modules.config.Global('topology-gocdb-connector.py', 'tests/global.conf')

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
        self.assertEqual(missing, set(['amsproject', 'amstopic']))
        merged = self.globalconfig.merge_opts(ams_incomplete, 'ams')
        self.assertEqual(merged, dict(amshost='host', amsproject='EGI', amstoken='token', amstopic='TOPIC'))

if __name__ == '__main__':
    unittest.main()
