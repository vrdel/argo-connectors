import unittest

from argo_egi_connectors.log import Logger
from argo_egi_connectors.mesh.storage_element_path import attach_sepath_topodata

from bonsai import LDAPEntry


logger = Logger('test_contactfeed.py')
CUSTOMER_NAME = 'CUSTOMERFOO'


class MeshSePathAndTopodata(unittest.TestCase):
    def setUp(self):
        logger.customer = CUSTOMER_NAME
        self.bdiiopts =  {
            'bdii': 'True', 'bdiihost': 'bdii.egi.cro-ngi.hr',
            'bdiiport': '2170',
            'bdiiqueryattributessepath': 'GlueVOInfoAccessControlBaseRule GlueVOInfoPath',
            'bdiiqueryattributessrm': 'GlueServiceEndpoint',
            'bdiiquerybase': 'o=grid',
            'bdiiqueryfiltersepath': '(objectClass=GlueSATop)',
            'bdiiqueryfiltersrm': '(&(objectClass=GlueService)(|(GlueServiceType=srm_v1)(GlueServiceType=srm)))'
        }


        self.sample_ldap = [
            {
                'GlueVOInfoAccessControlBaseRule': ['ukqcd.vo.gridpp.ac.uk', 'VO:ukqcd.vo.gridpp.ac.uk'],
                'GlueVOInfoPath': ['/dpm/gla.scotgrid.ac.uk/home/ukqcd.vo.gridpp.ac.uk'],
                'dn': 'GlueVOInfoLocalID=ukqcd.vo.gridpp.ac.uk:generalPool,GlueSALocalID=generalPool:replica:online,GlueSEUniqueID=svr018.gla.scotgrid.ac.uk,Mds-Vo-name=UKI-SCOTGRID-GLASGOW,Mds-Vo-name=local,o=grid'
            },
            {
                'GlueVOInfoAccessControlBaseRule': ['VO:atlas'],
                'GlueVOInfoPath': ['/atlas'],
                'dn': 'GlueVOInfoLocalID=atlas,GlueSALocalID=atlas:ATLASLOCALGROUPDISK,GlueSEUniqueID=lcg-se1.sfu.computecanada.ca,Mds-Vo-name=CA-SFU-T2,Mds-Vo-name=local,o=grid'
            }
        ]
        self.sample_storage_endpoints = [
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
            },
            {
                "type": "SITES", "group": "CA-SFU-T2", "service": "SRM",
                "hostname": "lcg-se1.sfu.computecanada.ca", "notifications": None,
                "tags": {"scope": "EGI", "monitored": "1", "production": "1",
                         "info_URL":
                         "httpg://lcg-se1.sfu.computecanada.ca:8443/srm/managerv2",
                         "info_SRM_port": "8443"
                }
            }
        ]
        self._construct_ldap_entries()

    def _construct_ldap_entries(self):
        tmp = list()
        for entry in self.sample_ldap:
            new_entry = LDAPEntry(entry['dn'])
            for key, value in entry.items():
                if key == 'dn':
                    continue
                new_entry[key] = value
            tmp.append(new_entry)
        self.sample_ldap = tmp

    def test_meshSePathTopo(self):
        import ipdb; ipdb.set_trace()
        new_endpoints = attach_sepath_topodata(logger, self.bdiiopts, self.sample_ldap, self.sample_storage_endpoints)


if __name__ == '__main__':
    unittest.main()
