import unittest

from argo_egi_connectors.log import Logger
from argo_egi_connectors.parse.gocdb_contacts import ParseSiteContacts, ParseSitesWithContacts, \
    ParseRocContacts, ParseServiceEndpointContacts, \
    ParseServiceGroupRoles, ParseServiceGroupWithContacts, ConnectorParseError
from argo_egi_connectors.parse.gocdb_topology import ParseServiceEndpoints


logger = Logger('test_contactfeed.py')
CUSTOMER_NAME = 'CUSTOMERFOO'


class MeshSePathAndTopodata(unittest.TestCase):
    def setUp(self):
        logger.customer = CUSTOMER_NAME
        self.sample_sepath_ldap = [
            {
                'GlueVOInfoAccessControlBaseRule': ['VO:vo.southgrid.ac.uk'],
                'GlueVOInfoPath': ['/dpm/phy.bris.ac.uk/home/vo.southgrid.ac.uk'],
                'dn': '<LDAPDN GlueVOInfoLocalID=vo.southgrid.ac.uk:hdfs_pool,GlueSALocalID=hdfs_pool:replica:online,GlueSEUniqueID=lcgse01.phy.bris.ac.uk,Mds-Vo-name=UKI-SOUTHGRID-BRIS-HEP,Mds-Vo-name=local,o=grid>'
            },
            {
                'GlueVOInfoAccessControlBaseRule': ['VO:gridpp'],
                'GlueVOInfoPath': ['/dpm/phy.bris.ac.uk/home/gridpp'],
                'dn': '<LDAPDN GlueVOInfoLocalID=gridpp:hdfs_pool,GlueSALocalID=hdfs_pool:replica:online,GlueSEUniqueID=lcgse01.phy.bris.ac.uk,Mds-Vo-name=UKI-SOUTHGRID-BRIS-HEP,Mds-Vo-name=local,o=grid>'
            }
        ]


    def test_meshSePathTopo(self):
        pass


if __name__ == '__main__':
    unittest.main()
