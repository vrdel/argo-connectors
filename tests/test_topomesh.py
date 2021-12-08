import unittest

from argo_egi_connectors.log import Logger
from argo_egi_connectors.mesh.storage_element_path import attach_sepath_topodata

from bonsai import LDAPEntry


logger = Logger('test_contactfeed.py')
CUSTOMER_NAME = 'CUSTOMERFOO'


class MeshSePathAndTopodata(unittest.TestCase):
    def setUp(self):
        logger.customer = CUSTOMER_NAME
        self.maxDiff = None
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
            },
            {
                'dn': 'GlueSALocalID=tape-,GlueSEUniqueID=srm.pic.es,Mds-Vo-name=pic,Mds-Vo-name=local,o=grid'
            },
            {
                'dn': 'GlueVOInfoLocalID=vo.cta.in2p3.fr:CTA,GlueSALocalID=CTA:SR:replica:online,GlueSEUniqueID=atlasse.lnf.infn.it,Mds-Vo-name=INFN-FRASCATI,Mds-Vo-name=local,o=grid',
                'GlueVOInfoAccessControlBaseRule': ['VOMS:/vo.cta.in2p3.fr/Role=production', 'VOMS:/vo.cta.in2p3.fr/Role=users'], 'GlueVOInfoPath': ['/dpm/lnf.infn.it/home/vo.cta.in2p3.fr']
            },
            {
                'dn': 'GlueVOInfoLocalID=default-store-ops,GlueSALocalID=nas-complex-7a759b03,GlueSEUniqueID=grid-se.physik.uni-wuppertal.de,Mds-Vo-name=wuppertalprod,Mds-Vo-name=local,o=grid',
                'GlueVOInfoAccessControlBaseRule': ['VO:ops'],
                'GlueVOInfoPath' : ['/pnfs/physik.uni-wuppertal.de/data/ops']
            },
            {
                'dn': 'GlueVOInfoLocalID=default-store-dteam,GlueSALocalID=nas-complex-7a759b03,GlueSEUniqueID=grid-se.physik.uni-wuppertal.de,Mds-Vo-name=wuppertalprod,Mds-Vo-name=local,o=grid',
                'GlueVOInfoAccessControlBaseRule': ['VO:dteam'],
                'GlueVOInfoPath': '/pnfs/physik.uni-wuppertal.de/data/dteam'
            },
            {
                'dn': 'GlueVOInfoLocalID=ops dteam:INFO-TOKEN,GlueSALocalID=info:replica:online,GlueSEUniqueID=se02.esc.qmul.ac.uk,Mds-Vo-name=UKI-LT2-QMUL,Mds-Vo-name=local,o=grid',
                'GlueVOInfoAccessControlBaseRule': ['VO:ops dteam'],
                'GlueVOInfoPath': '/info'
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
            },
            {
                "type": "SITES", "group": "wuppertalprod", "service": "SRM",
                "hostname": "grid-se.physik.uni-wuppertal.de",
                "notifications": None,
                "tags": {"scope": "EGI, wlcg, tier2, atlas", "monitored": "1",
                         "production": "1", "info_id": "3077G0",
                         "info_SRM_port": "8443"
                }
            },
            {
                "type": "SITES",
                "group": "UKI-LT2-QMUL",
                "service": "SRM",
                "hostname": "se02.esc.qmul.ac.uk",
                "notifications": None,
                "tags": {"scope": "EGI, wlcg, tier2, atlas, lhcb", "monitored": "1",
                         "production": "0", "info_id": "8458G0",
                         "info_SRM_port": "8444"
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
        attach_sepath_topodata(logger, self.bdiiopts, self.sample_ldap, self.sample_storage_endpoints)
        self.assertEqual(self.sample_storage_endpoints, [
            {
                'group': 'UKI-SCOTGRID-GLASGOW',
                'hostname': 'svr018.gla.scotgrid.ac.uk',
                'notifications': None,
                'service': 'eu.egi.storage.accounting',
                'tags': {
                    'monitored': '1',
                    'production': '1',
                    'scope': 'EGI, wlcg, tier2, atlas, cms, lhcb',
                    'vo_ukqcd.vo.gridpp.ac.uk_attr_GlueVOInfoPath': '/dpm/gla.scotgrid.ac.uk/home/ukqcd.vo.gridpp.ac.uk'
                },
                'type': 'SITES'
            },
            {
                'group': 'CA-WATERLOO-T2',
                'hostname': 'lcg-se1.uw.computecanada.ca',
                'notifications': None,
                'service': 'SRM',
                'tags': {
                    'info_SRM_port': '8443',
                    'info_URL': 'httpg://lcg-se1.uw.computecanada.ca:8443/srm/managerv2',
                    'monitored': '1',
                    'production': '1',
                    'scope': 'EGI'
                },
                'type': 'SITES'},
            {
                'group': 'CA-SFU-T2',
                'hostname': 'lcg-se1.sfu.computecanada.ca',
                'notifications': None,
                'service': 'SRM',
                'tags': {
                    'info_SRM_port': '8443',
                    'info_URL': 'httpg://lcg-se1.sfu.computecanada.ca:8443/srm/managerv2',
                    'monitored': '1',
                    'production': '1',
                    'scope': 'EGI',
                    'vo_atlas_attr_GlueVOInfoPath': '/atlas'
                },
                'type': 'SITES'
            },
            {
                'group': 'wuppertalprod',
                'hostname': 'grid-se.physik.uni-wuppertal.de',
                'notifications': None,
                'service': 'SRM',
                'tags': {
                    'info_SRM_port': '8443',
                    'info_id': '3077G0',
                    'monitored': '1',
                    'production': '1',
                    'scope': 'EGI, wlcg, tier2, atlas',
                    'vo_dteam_attr_GlueVOInfoPath': '/pnfs/physik.uni-wuppertal.de/data/dteam',
                    'vo_ops_attr_GlueVOInfoPath': '/pnfs/physik.uni-wuppertal.de/data/ops'
                },
                'type': 'SITES'
            },
            {
                'group': 'UKI-LT2-QMUL',
                'hostname': 'se02.esc.qmul.ac.uk',
                'notifications': None,
                'service': 'SRM',
                'tags': {'info_SRM_port': '8444',
                    'info_id': '8458G0',
                    'monitored': '1',
                    'production': '0',
                    'scope': 'EGI, wlcg, tier2, atlas, lhcb',
                    'vo_dteam_attr_GlueVOInfoPath': '/info',
                    'vo_ops_attr_GlueVOInfoPath': '/info'
                },
                'type': 'SITES'
            }])


if __name__ == '__main__':
    unittest.main()
