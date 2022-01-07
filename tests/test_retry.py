from argo_egi_connectors.io.http import SessionWithRetry
from aiohttp.test_utils import AioHTTPTestCase
from aiohttp import web


class ConnectorsHttpRetry(AioHTTPTestCase):
    def setUp(self):
        globopts = {
            'authenticationcafile': '/etc/pki/tls/certs/ca-bundle.crt',
            'authenticationcapath': '/etc/grid-security/certificates',
            'authenticationhostcert': '/etc/grid-security/hostcert.pem',
            'authenticationhostkey': '/etc/grid-security/hostkey.pem',
            'authenticationhttppass': 'xxxx', 'authenticationhttpuser': 'xxxx',
            'authenticationuseplainhttpauth': 'False',
            'authenticationverifyservercert': 'True', 'avroschemasweights':
            '/etc/argo-egi-connectors/schemas//weight_sites.avsc',
            'connectionretry': '3', 'connectionsleepretry': '15',
            'connectiontimeout': '180', 'generalpassextensions': 'True',
            'generalpublishwebapi': 'False', 'generalwriteavro': 'True',
            'inputstatedays': '3', 'inputstatesavedir':
            '/var/lib/argo-connectors/states/', 'outputweights':
            'weights_DATE.avro', 'webapihost': 'api.devel.argo.grnet.gr'
        }

        self.session = SessionWithRetry(logger, os.path.basename(sys.argv[0]),
                                        globopts, custauth=auth_opts)
        self.feed_parts = dict(
            scheme='https',
            netloc='operations-portal.egi.eu',
            path='/vapor/downloadLavoisier/option/json/view/VAPOR_Ngi_Sites_Info'
        )

        res = await session.http_get('{}://{}{}'.format(feed_parts.scheme,
                                                        feed_parts.netloc,
                                                        feed_parts.path))

    def test_WeightsRetry(self):
        pass

