import datetime
import httplib
import mock
import modules.config
import unittest2 as unittest

from bin.downtimes_gocdb_connector import GOCDBReader as DowntimesGOCDBReader
from bin.downtimes_gocdb_connector import main as downtimes_main
from bin.downtimes_gocdb_connector import argparse as downtimes_argparse
from bin.weights_vapor_connector import Vapor as VaporReader
from bin.topology_gocdb_connector import GOCDBReader, logger
from modules import input
from modules.helpers import module_class_name

class ConnectorSetup(object):
    downtimes_feed = \
        """<?xml version="1.0" encoding="UTF-8"?>\n<results>\n
           <DOWNTIME ID="22154" PRIMARY_KEY="100728G0" CLASSIFICATION="SCHEDULED">\n
               <PRIMARY_KEY>100728G0</PRIMARY_KEY>\n
               <HOSTNAME>nagios.c4.csir.co.za</HOSTNAME>\n
               <SERVICE_TYPE>ngi.SAM</SERVICE_TYPE>\n
               <ENDPOINT>nagios.c4.csir.co.zangi.SAM</ENDPOINT>\n
               <HOSTED_BY>ZA-MERAKA</HOSTED_BY>\n
               <GOCDB_PORTAL_URL>https://goc.egi.eu/portal/index.php?Page_Type=Downtime&amp;id=22154</GOCDB_PORTAL_URL>\n
               <AFFECTED_ENDPOINTS/>\n
               <SEVERITY>OUTAGE</SEVERITY>\n
               <DESCRIPTION>Preparation for decommissioning of the service.</DESCRIPTION>\n
               <INSERT_DATE>1481808624</INSERT_DATE>\n
               <START_DATE>1482105600</START_DATE>\n
               <END_DATE>1488240000</END_DATE>\n
               <FORMATED_START_DATE>2016-12-19 00:00</FORMATED_START_DATE>\n
               <FORMATED_END_DATE>2017-02-28 00:00</FORMATED_END_DATE>\n
           </DOWNTIME>\n
           <DOWNTIME ID="22209" PRIMARY_KEY="100784G0" CLASSIFICATION="SCHEDULED">\n
               <PRIMARY_KEY>100784G0</PRIMARY_KEY>\n
               <HOSTNAME>ce1.grid.lebedev.ru</HOSTNAME>\n
               <SERVICE_TYPE>CE</SERVICE_TYPE>\n
               <ENDPOINT>ce1.grid.lebedev.ruCE</ENDPOINT>\n
               <HOSTED_BY>ru-Moscow-FIAN-LCG2</HOSTED_BY>\n
               <GOCDB_PORTAL_URL>https://goc.egi.eu/portal/index.php?Page_Type=Downtime&amp;id=22209</GOCDB_PORTAL_URL>\n
               <AFFECTED_ENDPOINTS/>\n
               <SEVERITY>OUTAGE</SEVERITY>\n
               <DESCRIPTION>Problems with hosting room (hack for ATLAS site status script that does not currently handle site status and works only on DTs)</DESCRIPTION>\n
               <INSERT_DATE>1482748113</INSERT_DATE>\n
               <START_DATE>1482882540</START_DATE>\n
               <END_DATE>1485215940</END_DATE>\n
               <FORMATED_START_DATE>2016-12-27 23:49</FORMATED_START_DATE>\n
               <FORMATED_END_DATE>2017-01-23 23:59</FORMATED_END_DATE>\n
           </DOWNTIME>\n
           <DOWNTIME ID="22209" PRIMARY_KEY="100784G0" CLASSIFICATION="SCHEDULED">\n
               <PRIMARY_KEY>100784G0</PRIMARY_KEY>\n
               <HOSTNAME>ce1.grid.lebedev.ru</HOSTNAME>\n
               <SERVICE_TYPE>APEL</SERVICE_TYPE>\n
               <ENDPOINT>ce1.grid.lebedev.ruAPEL</ENDPOINT>\n
               <HOSTED_BY>ru-Moscow-FIAN-LCG2</HOSTED_BY>\n
               <GOCDB_PORTAL_URL>https://goc.egi.eu/portal/index.php?Page_Type=Downtime&amp;id=22209</GOCDB_PORTAL_URL>\n
               <AFFECTED_ENDPOINTS/>\n
               <SEVERITY>OUTAGE</SEVERITY>\n
               <DESCRIPTION>Problems with hosting room (hack for ATLAS site status script that does not currently handle site status and works only on DTs)</DESCRIPTION>\n
               <INSERT_DATE>1482748113</INSERT_DATE>\n
               <START_DATE>1482882540</START_DATE>\n
               <END_DATE>1485215940</END_DATE>\n
               <FORMATED_START_DATE>2016-12-27 23:49</FORMATED_START_DATE>\n
               <FORMATED_END_DATE>2017-01-23 23:59</FORMATED_END_DATE>\n
           </DOWNTIME>\n
           </results>\n"""

    poem = [{'metric': u'org.nordugrid.ARC-CE-ARIS',
             'profile': u'ch.cern.sam.ARGO_MON_CRITICAL',
             'service': u'ARC-CE',
             'tags': {'fqan': u'', 'vo': 'ops'}},
            {'metric': u'org.nordugrid.ARC-CE-IGTF',
             'profile': u'ch.cern.sam.ARGO_MON_CRITICAL',
             'service': u'ARC-CE',
             'tags': {'fqan': u'', 'vo': 'ops'}},
            {'metric': u'org.nordugrid.ARC-CE-result',
             'profile': u'ch.cern.sam.ARGO_MON_CRITICAL',
             'service': u'ARC-CE',
             'tags': {'fqan': u'', 'vo': 'ops'}}]

    downtimes = [{'end_time': '2017-01-19T23:59:00Z',
                  'hostname': u'nagios.c4.csir.co.za',
                  'service': u'ngi.SAM',
                  'start_time': '2017-01-19T00:00:00Z'},
                 {'end_time': '2017-01-19T23:59:00Z',
                  'hostname': u'ce1.grid.lebedev.ru',
                  'service': u'CE', 'start_time':
                  '2017-01-19T00:00:00Z'},
                 {'end_time': '2017-01-19T23:59:00Z',
                  'hostname': u'ce1.grid.lebedev.ru',
                  'service': u'APEL',
                  'start_time': '2017-01-19T00:00:00Z'}]

    weights = [{'site': u'FZK-LCG2', 'type': 'hepspec', 'weight': u'0'},
               {'site': u'IN2P3-IRES', 'type': 'hepspec', 'weight': u'13'},
               {'site': u'GRIF-LLR', 'type': 'hepspec', 'weight': u'0'}]

    group_groups = [{'group': u'AfricaArabia', 'subgroup': u'MA-01-CNRST',
                        'tags': {'certification': u'Certified',
                                'infrastructure': u'Production',
                                'scope': 'EGI'},
                        'type': 'NGI'},
                    {'group': u'AfricaArabia', 'subgroup': u'MA-04-CNRST-ATLAS',
                        'tags': {'certification': u'Certified',
                                'infrastructure': u'Production',
                                'scope': 'EGI'},
                        'type': 'NGI'},
                    {'group': u'AfricaArabia', 'subgroup': u'ZA-UCT-ICTS',
                        'tags': {'certification': u'Suspended',
                                'infrastructure': u'Production',
                                'scope': 'EGI'},
                        'type': 'NGI'}]

    group_endpoints = [{'group': u'100IT',
                        'hostname': u'occi-api.100percentit.com',
                        'service': u'eu.egi.cloud.vm-management.occi',
                        'tags': {'monitored': '1',
                                 'production': '1',
                                 'scope': 'EGI'},
                        'type': 'SITES'},
                        {'group': u'100IT',
                        'hostname': u'egi-cloud-accounting.100percentit.com',
                        'service': u'eu.egi.cloud.accounting',
                        'tags': {'monitored': '1',
                                 'production': '1',
                                 'scope': 'EGI'},
                        'type': 'SITES'},
                        {'group': u'100IT',
                        'hostname': u'occi-api.100percentit.com',
                        'service': u'eu.egi.cloud.information.bdii',
                        'tags': {'monitored': '1',
                                 'production': '1',
                                 'scope': 'EGI'},
                        'type': 'SITES'}]

    def __init__(self, connector, gconf, cconf):
        self.globalconfig = modules.config.Global(connector, gconf)
        self.customerconfig = modules.config.CustomerConf(connector, cconf)
        self.globopts = self.globalconfig.parse()
        self.customerconfig.parse()
        customers = self.customerconfig.get_customers()
        self.jobs = self.customerconfig.get_jobs(customers[0])
        self.jobdir = self.customerconfig.get_fulldir(customers[0], self.jobs[0])

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

class WeightsJson(unittest.TestCase):
    def setUp(self):
        self.connset = ConnectorSetup('weights-vapor-connector.py',
                                      'tests/global.conf',
                                      'tests/customer.conf')
        for c in ['globalconfig', 'customerconfig', 'globopts', 'jobs',
                  'jobdir', 'weights']:
            code = """self.%s = self.connset.%s""" % (c, c)
            exec code

    def wrap_get_weights(self, mock_conn):
        self.orig_get_weights.im_func.func_globals['globopts'] = self.globopts
        self.orig_get_weights.im_func.func_globals['input'].connection.func = mock_conn
        return self.orig_get_weights()

    @mock.patch('modules.input.connection')
    def testFailJson(self, mock_conn):
        feeds = self.customerconfig.get_mapfeedjobs('weights-vapor-connector.py',
                                                    deffeed= 'https://operations-portal.egi.eu/vapor/downloadLavoisier/option/json/view/VAPOR_Ngi_Sites_Info')
        vapor = VaporReader(feeds.keys()[0])
        datestamp = datetime.datetime.strptime('2017-01-19', '%Y-%m-%d')
        self.orig_get_weights = vapor.getWeights
        mock_conn.__name__ = 'mock_conn'
        mock_conn.return_value = 'Erroneous JSON feed'
        vapor.getWeights = self.wrap_get_weights
        self.assertEqual(vapor.getWeights(mock_conn), [])

class DowntimesXml(unittest.TestCase):
    def setUp(self):
        self.connset = ConnectorSetup('downtimes-gocdb-connector.py',
                                      'tests/global.conf',
                                      'tests/customer.conf')
        for c in ['globalconfig', 'customerconfig', 'globopts', 'jobs',
                  'jobdir', 'downtimes', 'downtimes_feed']:
            code = """self.%s = self.connset.%s""" % (c, c)
            exec code

    def wrap_get_downtimes(self, start, end, mock_conn):
        self.orig_get_downtimes.im_func.func_globals['globopts'] = self.globopts
        self.orig_get_downtimes.im_func.func_globals['input'].connection.func = mock_conn
        return self.orig_get_downtimes(start, end)

    @mock.patch('modules.input.connection')
    def testRetryConnection(self, mock_conn):
        feeds = self.customerconfig.get_mapfeedjobs('downtimes-gocdb-connector.py', deffeed='https://goc.egi.eu/gocdbpi/')
        gocdb = DowntimesGOCDBReader(feeds.keys()[0])
        datestamp = datetime.datetime.strptime('2017-01-19', '%Y-%m-%d')
        start = datestamp.replace(hour=0, minute=0, second=0)
        end = datestamp.replace(hour=23, minute=59, second=59)
        self.orig_get_downtimes = gocdb.getDowntimes
        gocdb.getDowntimes = self.wrap_get_downtimes
        mock_conn.__name__ = 'mock_conn'
        mock_conn.side_effect = [httplib.HTTPException('Bogus'),
                                 httplib.HTTPException('Bogus'),
                                 httplib.HTTPException('Bogus')]
        self.assertEqual(gocdb.getDowntimes(start, end, mock_conn), [])
        self.assertEqual(mock_conn.call_count, int(self.globopts['ConnectionRetry'.lower()]) + 1)

    @mock.patch('modules.input.connection')
    def testXml(self, mock_conn):
        feeds = self.customerconfig.get_mapfeedjobs('downtimes-gocdb-connector.py', deffeed='https://goc.egi.eu/gocdbpi/')
        gocdb = DowntimesGOCDBReader(feeds.keys()[0])
        datestamp = datetime.datetime.strptime('2017-01-19', '%Y-%m-%d')
        start = datestamp.replace(hour=0, minute=0, second=0)
        end = datestamp.replace(hour=23, minute=59, second=59)
        self.orig_get_downtimes = gocdb.getDowntimes
        mock_conn.__name__ = 'mock_conn'
        mock_conn.return_value = 'Erroneous XML feed'
        gocdb.getDowntimes = self.wrap_get_downtimes
        self.assertEqual(gocdb.getDowntimes(start, end, mock_conn), [])
        mock_conn.return_value = self.downtimes_feed
        gocdb.getDowntimes = self.wrap_get_downtimes
        self.assertEqual(gocdb.getDowntimes(start, end, mock_conn), self.downtimes)

    @mock.patch('bin.downtimes_gocdb_connector.sys')
    @mock.patch('modules.output.write_state')
    @mock.patch('bin.downtimes_gocdb_connector.CustomerConf', autospec=True)
    @mock.patch('bin.downtimes_gocdb_connector.argparse.ArgumentParser.parse_args')
    @mock.patch('bin.downtimes_gocdb_connector.Global')
    @mock.patch('bin.downtimes_gocdb_connector.GOCDBReader')
    def testStateFile(self, gocdbreader, glob, parse_args, customerconf, write_state, mock_sys):
        argmock = mock.Mock()
        argmock.date = ['2017-01-19']
        argmock.gloconf = ['tests/global.conf']
        argmock.custconf = ['tests/customer.conf']
        parse_args.return_value = argmock
        customerconf.get_mapfeedjobs.return_value = self.customerconfig.get_mapfeedjobs('downtimes-gocdb-connector.py',
                                                                                        deffeed='https://goc.egi.eu/gocdbpi/')
        customerconf.get_fullstatedir.side_effect = ['/var/lib/argo-connectors/states//EGI/EGI_Critical', '/var/lib/argo-connectors/states//EGI/EGI_Cloudmon', '/var/lib/argo-connectors/states//EGI/EGI_Critical', '/var/lib/argo-connectors/states//EGI/EGI_Cloudmon']
        self.globopts['generalwriteavro'] = 'False'
        mock_sys.argv = ['downtimes-gocdb-connector.py']
        downtimes_main.func_globals['output'].write_state = write_state
        customerconf.side_effect = [customerconf, customerconf]
        gocdbreader.side_effect = [gocdbreader, gocdbreader]
        glob.side_effect = [glob, glob]
        glob.is_complete.return_value = (True, [])

        gocdbreader.state = True
        gocdbreader.getDowntimes.return_value = self.downtimes
        glob.parse.return_value = self.globopts
        downtimes_main()
        self.assertTrue(write_state.called)
        self.assertEqual(write_state.call_count, len(self.jobs))
        for call in write_state.call_args_list:
            self.assertTrue(gocdbreader.state in call[0])
            self.assertTrue('2017_01_19' in call[0])

        gocdbreader.state = False
        gocdbreader.getDowntimes.return_value = []
        downtimes_main()
        self.assertTrue(write_state.called)
        self.assertEqual(write_state.call_count, 2*len(self.jobs))
        for call in write_state.call_args_list[2:]:
            self.assertTrue(gocdbreader.state in call[0])
            self.assertTrue('2017_01_19' in call[0])


if __name__ == '__main__':
    unittest.main()
