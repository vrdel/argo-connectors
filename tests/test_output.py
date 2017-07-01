import json
import mock
import modules.config
import unittest2 as unittest

from bin.topology_gocdb_connector import logger

from httmock import urlmatch, HTTMock, response
from modules import output
from modules.helpers import datestamp, filename_date


class ConnectorSetup(object):
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


class TopologyAvro(unittest.TestCase):
    def setUp(self):
        self.connset = ConnectorSetup('topology-gocdb-connector.py',
                                      'tests/global.conf',
                                      'tests/customer.conf')
        for c in ['globalconfig', 'customerconfig', 'globopts', 'jobs',
                  'jobdir', 'group_groups', 'group_endpoints']:
            code = """self.%s = self.connset.%s""" % (c, c)
            exec code

    @mock.patch('modules.output.load_schema')
    @mock.patch('modules.output.open')
    def testGroupGroups(self, mock_open, mock_lschema):
        mock_avrofile = mock.create_autospec(output.DataFileWriter)
        filename = filename_date(logger, self.globopts['OutputTopologyGroupOfGroups'.lower()], self.jobdir)
        m = output.AvroWriter(self.globopts['AvroSchemasTopologyGroupOfGroups'.lower()], filename)
        m.datawrite = mock_avrofile
        m.write(self.group_groups)
        mock_open.assert_called_with(filename, 'w+')
        mock_lschema.assert_called_with(self.globopts['AvroSchemasTopologyGroupOfGroups'.lower()])
        self.assertTrue(mock_avrofile.append.called)
        self.assertEqual(mock_avrofile.append.call_count, 3)
        self.assertEqual(mock_avrofile.append.mock_calls.index(mock.call(self.group_groups[0])), 0)
        self.assertEqual(mock_avrofile.append.mock_calls.index(mock.call(self.group_groups[1])), 1)
        self.assertEqual(mock_avrofile.append.mock_calls.index(mock.call(self.group_groups[2])), 2)


class DowntimesAvro(unittest.TestCase):
    def setUp(self):
        self.connset = ConnectorSetup('downtimes-gocdb-connector.py',
                                      'tests/global.conf',
                                      'tests/customer.conf')
        for c in ['globalconfig', 'customerconfig', 'globopts', 'jobs',
                  'jobdir', 'downtimes']:
            code = """self.%s = self.connset.%s""" % (c, c)
            exec code

    @mock.patch('modules.output.load_schema')
    @mock.patch('modules.output.open')
    def testDowntimes(self, mock_open, mock_lschema):
        mock_avrofile = mock.create_autospec(output.DataFileWriter)
        filename = filename_date(logger, self.globopts['OutputDowntimes'.lower()], self.jobdir)
        m = output.AvroWriter(self.globopts['AvroSchemasDowntimes'.lower()], filename)
        m.datawrite = mock_avrofile
        m.write(self.downtimes)
        mock_open.assert_called_with(filename, 'w+')
        mock_lschema.assert_called_with(self.globopts['AvroSchemasDowntimes'.lower()])
        self.assertTrue(mock_avrofile.append.called)
        self.assertEqual(mock_avrofile.append.call_count, 3)
        self.assertEqual(mock_avrofile.append.mock_calls.index(mock.call(self.downtimes[0])), 0)
        self.assertEqual(mock_avrofile.append.mock_calls.index(mock.call(self.downtimes[1])), 1)
        self.assertEqual(mock_avrofile.append.mock_calls.index(mock.call(self.downtimes[2])), 2)


class WeightsAvro(unittest.TestCase):
    def setUp(self):
        self.connset = ConnectorSetup('weights-vapor-connector.py',
                                      'tests/global.conf',
                                      'tests/customer.conf')
        for c in ['globalconfig', 'customerconfig', 'globopts', 'jobs',
                  'jobdir', 'weights']:
            code = """self.%s = self.connset.%s""" % (c, c)
            exec code

    @mock.patch('modules.output.load_schema')
    @mock.patch('modules.output.open')
    def testWeights(self, mock_open, mock_lschema):
        mock_avrofile = mock.create_autospec(output.DataFileWriter)
        filename = filename_date(logger, self.globopts['OutputWeights'.lower()], self.jobdir)
        m = output.AvroWriter(self.globopts['AvroSchemasWeights'.lower()], filename)
        m.datawrite = mock_avrofile
        m.write(self.weights)
        mock_open.assert_called_with(filename, 'w+')
        mock_lschema.assert_called_with(self.globopts['AvroSchemasWeights'.lower()])
        self.assertTrue(mock_avrofile.append.called)
        self.assertEqual(mock_avrofile.append.call_count, 3)
        self.assertEqual(mock_avrofile.append.mock_calls.index(mock.call(self.weights[0])), 0)
        self.assertEqual(mock_avrofile.append.mock_calls.index(mock.call(self.weights[1])), 1)
        self.assertEqual(mock_avrofile.append.mock_calls.index(mock.call(self.weights[2])), 2)


class WeightsAvro(unittest.TestCase):
    def setUp(self):
        self.connset = ConnectorSetup('poem-connector.py',
                                      'tests/global.conf',
                                      'tests/customer.conf')
        for c in ['globalconfig', 'customerconfig', 'globopts', 'jobs',
                  'jobdir', 'poem']:
            code = """self.%s = self.connset.%s""" % (c, c)
            exec code

    @mock.patch('modules.output.load_schema')
    @mock.patch('modules.output.open')
    def testWeights(self, mock_open, mock_lschema):
        mock_avrofile = mock.create_autospec(output.DataFileWriter)
        filename = filename_date(logger, self.globopts['OutputPoem'.lower()], self.jobdir)
        m = output.AvroWriter(self.globopts['AvroSchemasPoem'.lower()], filename)
        m.datawrite = mock_avrofile
        m.write(self.poem)
        mock_open.assert_called_with(filename, 'w+')
        mock_lschema.assert_called_with(self.globopts['AvroSchemasPoem'.lower()])
        self.assertTrue(mock_avrofile.append.called)
        self.assertEqual(mock_avrofile.append.call_count, 3)
        self.assertEqual(mock_avrofile.append.mock_calls.index(mock.call(self.poem[0])), 0)
        self.assertEqual(mock_avrofile.append.mock_calls.index(mock.call(self.poem[1])), 1)
        self.assertEqual(mock_avrofile.append.mock_calls.index(mock.call(self.poem[2])), 2)


class PoemAms(unittest.TestCase):
    get_topic_urlmatch = dict(netloc='localhost',
                              path='/v1/projects/EGI/topics/TOPIC',
                              method='GET')

    publish_topic_urlmatch = dict(netloc='localhost',
                                  path='/v1/projects/EGI/topics/TOPIC:publish',
                                  method='POST')

    def setUp(self):
        self.connset = ConnectorSetup('poem-connector.py',
                                      'tests/global.conf',
                                      'tests/customer.conf')
        for c in ['globalconfig', 'customerconfig', 'globopts', 'jobs',
                  'jobdir', 'poem']:
            code = """self.%s = self.connset.%s""" % (c, c)
            exec code

        self.amspublish = output.AmsPublish(self.globopts['amshost'],
                                            self.globopts['amsproject'],
                                            self.globopts['amstoken'],
                                            self.globopts['amstopic'],
                                            self.customerconfig.get_jobdir(self.jobs[0]),
                                            self.globopts['amsbulk'],
                                            logger,
                                            int(self.globopts['connectionretry']),
                                            int(self.globopts['connectiontimeout']))

    def testPoem(self):
        @urlmatch(**self.get_topic_urlmatch)
        def get_topic_mock(url, request):
            # Return the details of a topic in json format
            return response(200, '{"name": "/projects/EGI/topics/TOPIC"}', None, None, 5, request)

        @urlmatch(**self.publish_topic_urlmatch)
        def publish_bulk_mock(url, request):
            assert url.path == "/v1/projects/EGI/topics/TOPIC:publish"
            # Check request produced by ams client
            req_body = json.loads(request.body)
            self.assertEqual(req_body["messages"][0]["data"], "OmNoLmNlcm4uc2FtLkFSR09fTU9OX0NSSVRJQ0FMDEFSQy1DRTJvcmcubm9yZHVncmlkLkFSQy1DRS1BUklTAgQIZnFhbgAEdm8Gb3BzAA==")
            self.assertEqual(req_body["messages"][0]["attributes"]["type"], "poem")
            self.assertEqual(req_body["messages"][0]["attributes"]["report"], "EGI_Critical")
            self.assertEqual(req_body["messages"][0]["attributes"]["partition_date"], datestamp().replace('_', '-'))

            self.assertEqual(req_body["messages"][1]["data"], "OmNoLmNlcm4uc2FtLkFSR09fTU9OX0NSSVRJQ0FMDEFSQy1DRTJvcmcubm9yZHVncmlkLkFSQy1DRS1JR1RGAgQIZnFhbgAEdm8Gb3BzAA==")
            self.assertEqual(req_body["messages"][1]["attributes"]["type"], "poem")
            self.assertEqual(req_body["messages"][1]["attributes"]["report"], "EGI_Critical")
            self.assertEqual(req_body["messages"][1]["attributes"]["partition_date"], datestamp().replace('_', '-'))

            self.assertEqual(req_body["messages"][2]["data"], "OmNoLmNlcm4uc2FtLkFSR09fTU9OX0NSSVRJQ0FMDEFSQy1DRTZvcmcubm9yZHVncmlkLkFSQy1DRS1yZXN1bHQCBAhmcWFuAAR2bwZvcHMA")
            self.assertEqual(req_body["messages"][2]["attributes"]["type"], "poem")
            self.assertEqual(req_body["messages"][2]["attributes"]["report"], "EGI_Critical")
            self.assertEqual(req_body["messages"][2]["attributes"]["partition_date"], datestamp().replace('_', '-'))

            return '{"msgIds": ["1", "2", "3"]}'


        with HTTMock(get_topic_mock, publish_bulk_mock):
            ret = self.amspublish.send(self.globopts['AvroSchemasPoem'.lower()],
                                 'poem', datestamp().replace('_', '-'),
                                 self.poem)
            self.assertTrue(ret)


class WeightsAms(unittest.TestCase):
    get_topic_urlmatch = dict(netloc='localhost',
                              path='/v1/projects/EGI/topics/TOPIC',
                              method='GET')

    publish_topic_urlmatch = dict(netloc='localhost',
                                  path='/v1/projects/EGI/topics/TOPIC:publish',
                                  method='POST')

    def setUp(self):
        self.connset = ConnectorSetup('weights-vapor-connector.py',
                                      'tests/global.conf',
                                      'tests/customer.conf')
        for c in ['globalconfig', 'customerconfig', 'globopts', 'jobs',
                  'jobdir', 'weights']:
            code = """self.%s = self.connset.%s""" % (c, c)
            exec code

        self.amspublish = output.AmsPublish(self.globopts['amshost'],
                                            self.globopts['amsproject'],
                                            self.globopts['amstoken'],
                                            self.globopts['amstopic'],
                                            self.customerconfig.get_jobdir(self.jobs[0]),
                                            self.globopts['amsbulk'],
                                            logger,
                                            int(self.globopts['connectionretry']),
                                            int(self.globopts['connectiontimeout']))

    def testWeights(self):
        @urlmatch(**self.get_topic_urlmatch)
        def get_topic_mock(url, request):
            # Return the details of a topic in json format
            return response(200, '{"name": "/projects/EGI/topics/TOPIC"}', None, None, 5, request)

        @urlmatch(**self.publish_topic_urlmatch)
        def publish_bulk_mock(url, request):
            assert url.path == "/v1/projects/EGI/topics/TOPIC:publish"
            # Check request produced by ams client
            req_body = json.loads(request.body)
            self.assertEqual(req_body["messages"][0]["data"], "DmhlcHNwZWMQRlpLLUxDRzICMA==")
            self.assertEqual(req_body["messages"][0]["attributes"]["type"], "weights")
            self.assertEqual(req_body["messages"][0]["attributes"]["report"], "EGI_Critical")
            self.assertEqual(req_body["messages"][0]["attributes"]["partition_date"], datestamp().replace('_', '-'))

            self.assertEqual(req_body["messages"][1]["data"], "DmhlcHNwZWMUSU4yUDMtSVJFUwQxMw==")
            self.assertEqual(req_body["messages"][1]["attributes"]["type"], "weights")
            self.assertEqual(req_body["messages"][1]["attributes"]["report"], "EGI_Critical")
            self.assertEqual(req_body["messages"][1]["attributes"]["partition_date"], datestamp().replace('_', '-'))

            self.assertEqual(req_body["messages"][2]["data"], "DmhlcHNwZWMQR1JJRi1MTFICMA==")
            self.assertEqual(req_body["messages"][2]["attributes"]["type"], "weights")
            self.assertEqual(req_body["messages"][2]["attributes"]["report"], "EGI_Critical")
            self.assertEqual(req_body["messages"][2]["attributes"]["partition_date"], datestamp().replace('_', '-'))

            return '{"msgIds": ["1", "2", "3"]}'


        with HTTMock(get_topic_mock, publish_bulk_mock):
            ret = self.amspublish.send(self.globopts['AvroSchemasWeights'.lower()],
                                 'weights', datestamp().replace('_', '-'),
                                 self.weights)
            self.assertTrue(ret)


class DowntimesAms(unittest.TestCase):
    get_topic_urlmatch = dict(netloc='localhost',
                              path='/v1/projects/EGI/topics/TOPIC',
                              method='GET')

    publish_topic_urlmatch = dict(netloc='localhost',
                                  path='/v1/projects/EGI/topics/TOPIC:publish',
                                  method='POST')

    def setUp(self):
        self.connset = ConnectorSetup('downtimes-gocdb-connector.py',
                                      'tests/global.conf',
                                      'tests/customer.conf')
        for c in ['globalconfig', 'customerconfig', 'globopts', 'jobs',
                  'jobdir', 'downtimes']:
            code = """self.%s = self.connset.%s""" % (c, c)
            exec code

        self.amspublish = output.AmsPublish(self.globopts['amshost'],
                                            self.globopts['amsproject'],
                                            self.globopts['amstoken'],
                                            self.globopts['amstopic'],
                                            self.customerconfig.get_jobdir(self.jobs[0]),
                                            self.globopts['amsbulk'],
                                            logger,
                                            int(self.globopts['connectionretry']),
                                            int(self.globopts['connectiontimeout']))

    def testDowntimes(self):
        @urlmatch(**self.get_topic_urlmatch)
        def get_topic_mock(url, request):
            # Return the details of a topic in json format
            return response(200, '{"name": "/projects/EGI/topics/TOPIC"}', None, None, 5, request)

        @urlmatch(**self.publish_topic_urlmatch)
        def publish_bulk_mock(url, request):
            assert url.path == "/v1/projects/EGI/topics/TOPIC:publish"
            # Check request produced by ams client
            req_body = json.loads(request.body)
            self.assertEqual(req_body["messages"][0]["data"], "KG5hZ2lvcy5jNC5jc2lyLmNvLnphDm5naS5TQU0oMjAxNy0wMS0xOVQwMDowMDowMFooMjAxNy0wMS0xOVQyMzo1OTowMFo=")
            self.assertEqual(req_body["messages"][0]["attributes"]["type"], "downtimes")
            self.assertEqual(req_body["messages"][0]["attributes"]["report"], "EGI_Critical")
            self.assertEqual(req_body["messages"][0]["attributes"]["partition_date"], datestamp().replace('_', '-'))

            self.assertEqual(req_body["messages"][1]["data"], "JmNlMS5ncmlkLmxlYmVkZXYucnUEQ0UoMjAxNy0wMS0xOVQwMDowMDowMFooMjAxNy0wMS0xOVQyMzo1OTowMFo=")
            self.assertEqual(req_body["messages"][1]["attributes"]["type"], "downtimes")
            self.assertEqual(req_body["messages"][1]["attributes"]["report"], "EGI_Critical")
            self.assertEqual(req_body["messages"][1]["attributes"]["partition_date"], datestamp().replace('_', '-'))

            self.assertEqual(req_body["messages"][2]["data"], "JmNlMS5ncmlkLmxlYmVkZXYucnUIQVBFTCgyMDE3LTAxLTE5VDAwOjAwOjAwWigyMDE3LTAxLTE5VDIzOjU5OjAwWg==")
            self.assertEqual(req_body["messages"][2]["attributes"]["type"], "downtimes")
            self.assertEqual(req_body["messages"][2]["attributes"]["report"], "EGI_Critical")
            self.assertEqual(req_body["messages"][2]["attributes"]["partition_date"], datestamp().replace('_', '-'))

            return '{"msgIds": ["1", "2", "3"]}'


        with HTTMock(get_topic_mock, publish_bulk_mock):
            ret = self.amspublish.send(self.globopts['AvroSchemasDowntimes'.lower()],
                                 'downtimes', datestamp().replace('_', '-'),
                                 self.downtimes)
            self.assertTrue(ret)


class TopologyAms(unittest.TestCase):
    get_topic_urlmatch = dict(netloc='localhost',
                              path='/v1/projects/EGI/topics/TOPIC',
                              method='GET')

    publish_topic_urlmatch = dict(netloc='localhost',
                                  path='/v1/projects/EGI/topics/TOPIC:publish',
                                  method='POST')

    def setUp(self):
        self.connset = ConnectorSetup('topology-gocdb-connector.py',
                                      'tests/global.conf',
                                      'tests/customer.conf')
        for c in ['globalconfig', 'customerconfig', 'globopts', 'jobs',
                  'jobdir', 'group_groups', 'group_endpoints']:
            code = """self.%s = self.connset.%s""" % (c, c)
            exec code

        self.amspublish = output.AmsPublish(self.globopts['amshost'],
                                            self.globopts['amsproject'],
                                            self.globopts['amstoken'],
                                            self.globopts['amstopic'],
                                            self.customerconfig.get_jobdir(self.jobs[0]),
                                            self.globopts['amsbulk'],
                                            logger,
                                            int(self.globopts['connectionretry']),
                                            int(self.globopts['connectiontimeout']))

    def testGroupGroups(self):
        @urlmatch(**self.get_topic_urlmatch)
        def get_topic_mock(url, request):
            # Return the details of a topic in json format
            return response(200, '{"name": "/projects/EGI/topics/TOPIC"}', None, None, 5, request)

        @urlmatch(**self.publish_topic_urlmatch)
        def publish_bulk_mock(url, request):
            assert url.path == "/v1/projects/EGI/topics/TOPIC:publish"
            # Check request produced by ams client
            req_body = json.loads(request.body)
            self.assertEqual(req_body["messages"][0]["data"], "Bk5HSRhBZnJpY2FBcmFiaWEWTUEtMDEtQ05SU1QCBgpzY29wZQZFR0kcaW5mcmFzdHJ1Y3R1cmUUUHJvZHVjdGlvbhpjZXJ0aWZpY2F0aW9uEkNlcnRpZmllZAA=")
            self.assertEqual(req_body["messages"][0]["attributes"]["type"], "group_groups")
            self.assertEqual(req_body["messages"][0]["attributes"]["report"], "EGI_Critical")
            self.assertEqual(req_body["messages"][0]["attributes"]["partition_date"], datestamp().replace('_', '-'))

            self.assertEqual(req_body["messages"][1]["data"], "Bk5HSRhBZnJpY2FBcmFiaWEiTUEtMDQtQ05SU1QtQVRMQVMCBgpzY29wZQZFR0kcaW5mcmFzdHJ1Y3R1cmUUUHJvZHVjdGlvbhpjZXJ0aWZpY2F0aW9uEkNlcnRpZmllZAA=")
            self.assertEqual(req_body["messages"][1]["attributes"]["type"], "group_groups")
            self.assertEqual(req_body["messages"][1]["attributes"]["report"], "EGI_Critical")
            self.assertEqual(req_body["messages"][1]["attributes"]["partition_date"], datestamp().replace('_', '-'))

            self.assertEqual(req_body["messages"][2]["data"], "Bk5HSRhBZnJpY2FBcmFiaWEWWkEtVUNULUlDVFMCBgpzY29wZQZFR0kcaW5mcmFzdHJ1Y3R1cmUUUHJvZHVjdGlvbhpjZXJ0aWZpY2F0aW9uElN1c3BlbmRlZAA=")
            self.assertEqual(req_body["messages"][2]["attributes"]["type"], "group_groups")
            self.assertEqual(req_body["messages"][2]["attributes"]["report"], "EGI_Critical")
            self.assertEqual(req_body["messages"][2]["attributes"]["partition_date"], datestamp().replace('_', '-'))

            return '{"msgIds": ["1", "2", "3"]}'


        with HTTMock(get_topic_mock, publish_bulk_mock):
            ret = self.amspublish.send(self.globopts['AvroSchemasTopologyGroupOfGroups'.lower()],
                                       'group_groups', datestamp().replace('_', '-'), self.group_groups)
            self.assertTrue(ret)

    def testGroupEndpoints(self):
        @urlmatch(**self.get_topic_urlmatch)
        def get_topic_mock(url, request):
            # Return the details of a topic in json format
            return response(200, '{"name": "/projects/EGI/topics/TOPIC"}', None, None, 5, request)

        @urlmatch(**self.publish_topic_urlmatch)
        def publish_bulk_mock(url, request):
            assert url.path == "/v1/projects/EGI/topics/TOPIC:publish"
            # Check request produced by ams client
            req_body = json.loads(request.body)
            self.assertEqual(req_body["messages"][0]["data"], "ClNJVEVTCjEwMElUPmV1LmVnaS5jbG91ZC52bS1tYW5hZ2VtZW50Lm9jY2kyb2NjaS1hcGkuMTAwcGVyY2VudGl0LmNvbQIGCnNjb3BlBkVHSRRwcm9kdWN0aW9uAjESbW9uaXRvcmVkAjEA")
            self.assertEqual(req_body["messages"][0]["attributes"]["type"], "group_endpoints")
            self.assertEqual(req_body["messages"][0]["attributes"]["report"], "EGI_Critical")
            self.assertEqual(req_body["messages"][0]["attributes"]["partition_date"], datestamp().replace('_', '-'))

            self.assertEqual(req_body["messages"][1]["data"], "ClNJVEVTCjEwMElULmV1LmVnaS5jbG91ZC5hY2NvdW50aW5nSmVnaS1jbG91ZC1hY2NvdW50aW5nLjEwMHBlcmNlbnRpdC5jb20CBgpzY29wZQZFR0kUcHJvZHVjdGlvbgIxEm1vbml0b3JlZAIxAA==")
            self.assertEqual(req_body["messages"][1]["attributes"]["type"], "group_endpoints")
            self.assertEqual(req_body["messages"][1]["attributes"]["report"], "EGI_Critical")
            self.assertEqual(req_body["messages"][1]["attributes"]["partition_date"], datestamp().replace('_', '-'))

            self.assertEqual(req_body["messages"][2]["data"], "ClNJVEVTCjEwMElUOmV1LmVnaS5jbG91ZC5pbmZvcm1hdGlvbi5iZGlpMm9jY2ktYXBpLjEwMHBlcmNlbnRpdC5jb20CBgpzY29wZQZFR0kUcHJvZHVjdGlvbgIxEm1vbml0b3JlZAIxAA==")
            self.assertEqual(req_body["messages"][2]["attributes"]["type"], "group_endpoints")
            self.assertEqual(req_body["messages"][2]["attributes"]["report"], "EGI_Critical")
            self.assertEqual(req_body["messages"][2]["attributes"]["partition_date"], datestamp().replace('_', '-'))

            return '{"msgIds": ["1", "2", "3"]}'


        with HTTMock(get_topic_mock, publish_bulk_mock):
            ret = self.amspublish.send(self.globopts['AvroSchemasTopologyGroupOfEndpoints'.lower()],
                                       'group_endpoints', datestamp().replace('_', '-'), self.group_endpoints)
            self.assertTrue(ret)
