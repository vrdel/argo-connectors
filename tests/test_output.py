import json
import modules.config
import unittest

from httmock import urlmatch, HTTMock, response
from modules import output
from modules.helpers import module_class_name, datestamp

class TopologyAms(unittest.TestCase):
    get_topic_urlmatch = dict(netloc='localhost',
                                path='/v1/projects/EGI/topics/TOPIC',
                                method='GET')

    publish_topic_urlmatch = dict(netloc='localhost',
                                  path='/v1/projects/EGI/topics/TOPIC:publish',
                                  method='POST')

    def setUp(self):
        self.globalconfig = modules.config.Global('topology-gocdb-connector.py', 'tests/global.conf')
        self.customerconfig = modules.config.CustomerConf('topology-gocdb-connector.py', 'tests/customer.conf')
        self.globopts = self.globalconfig.parse()
        self.customerconfig.parse()
        customers = self.customerconfig.get_customers()
        jobs = self.customerconfig.get_jobs(customers[0])
        jobdir = self.customerconfig.get_jobdir(jobs[0])
        self.amspublish = output.AmsPublish(self.globopts['amshost'],
                                            self.globopts['amsproject'],
                                            self.globopts['amstoken'],
                                            self.globopts['amstopic'],
                                            jobdir,
                                            self.globopts['amsbulk'],
                                            int(self.globopts['connectiontimeout']))

    def testGroupGroups(self):
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
            ret, excep = self.amspublish.send(self.globopts['AvroSchemasTopologyGroupOfGroups'.lower()],
                                            'group_groups', datestamp().replace('_', '-'), group_groups)
            self.assertTrue(ret)

    def testGroupEndpoints(self):
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
            ret, excep = self.amspublish.send(self.globopts['AvroSchemasTopologyGroupOfEndpoints'.lower()],
                                            'group_endpoints', datestamp().replace('_', '-'), group_endpoints)
            self.assertTrue(ret)

