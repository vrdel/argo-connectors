import unittest

from argo_egi_connectors.log import Logger
from argo_egi_connectors.parse.gocdb_contacts import ParseSiteContacts, ParseSitesWithContacts, \
    ParseRocContacts, ParseServiceEndpointContacts, \
    ParseServiceGroupRoles, ParseServiceGroupWithContacts, ConnectorParseError
from argo_egi_connectors.parse.gocdb_topology import ParseServiceEndpoints

from bonsai import LDAPEntry


logger = Logger('test_contactfeed.py')
CUSTOMER_NAME = 'CUSTOMERFOO'


class MeshSePathAndTopodata(unittest.TestCase):
    def setUp(self):
        logger.customer = CUSTOMER_NAME
        self.sample_ldap = [
            {
                'GlueVOInfoAccessControlBaseRule': ['VO:ukqcd.vo.gridpp.ac.uk'],
                'GlueVOInfoPath': ['/dpm/gla.scotgrid.ac.uk/home/ukqcd.vo.gridpp.ac.uk'],
                'dn': 'GlueVOInfoLocalID=ukqcd.vo.gridpp.ac.uk:generalPool,GlueSALocalID=generalPool:replica:online,GlueSEUniqueID=svr018.gla.scotgrid.ac.uk,Mds-Vo-name=UKI-SCOTGRID-GLASGOW,Mds-Vo-name=local,o=grid'
            },
            {
                'GlueVOInfoAccessControlBaseRule': ['VO:atlas'],
                'GlueVOInfoPath': ['/atlas'],
                'dn': 'GlueVOInfoLocalID=atlas,GlueSALocalID=atlas:ATLASLOCALGROUPDISK,GlueSEUniqueID=lcg-se1.sfu.computecanada.ca,Mds-Vo-name=CA-SFU-T2,Mds-Vo-name=local,o=grid'
            }
        ]
        self.sample_gridftp_endpoints = [
            {
                "type": "SITES", "group": "UKI-SCOTGRID-GLASGOW", "service":
                "eu.egi.storage.accounting", "hostname":
                "svr018.gla.scotgrid.ac.uk", "notifications": None, "tags":
                {
                    "scope": "EGI, wlcg, tier2, atlas, cms, lhcb",
                    "monitored": "1",
                    "production": "1"
                }
            },
            {
                "type": "SITES", "group": "CA-WATERLOO-T2", "service": "SRM",
                "hostname": "lcg-se1.uw.computecanada.ca", "notifications": None,
                "tags": {
                    "scope": "EGI",
                    "monitored": "1",
                    "production": "1",
                    "info_URL": "httpg://lcg-se1.uw.computecanada.ca:8443/srm/managerv2",
                    "info_SRM_port": "8443"
                }
            }
        ]
        self.construct_ldap_entries()


    def construct_ldap_entries(self):
        tmp = list()
        for entry in self.sample_ldap:
            new_entry = LDAPEntry(entry['dn'])
            for key, value in entry.items():
                if key == 'dn':
                    continue
                new_entry[key] = value
            tmp.append(new_entry)
        self.sample_ldap = tmp

    def extract_value(self, key, entry):
        if isinstance(entry, tuple):
            for e in entry:
                k, v = e[0]
                if key == k:
                    return v
        else:
            return entry[key]

    def test_meshSePathTopo(self):
        for entry in self.sample_ldap:
            voname = self.extract_value('GlueVOInfoAccessControlBaseRule', entry)
            vopath = self.extract_value('GlueVOInfoPath', entry)
            endpoint = self.extract_value('GlueSEUniqueID', entry['dn'].rdns)
            import ipdb; ipdb.set_trace()


if __name__ == '__main__':
    unittest.main()
