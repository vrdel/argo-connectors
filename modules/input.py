import httplib
import json
import socket
import xml.dom.minidom
import os

from xml.parsers.expat import ExpatError
from time import sleep

from argo_egi_connectors.helpers import error_message, retry

from OpenSSL.SSL import Error as SSLError
from OpenSSL.SSL import TLSv1_METHOD, Context, Connection
from OpenSSL.SSL import VERIFY_PEER
from OpenSSL.SSL import WantReadError as SSLWantReadError

class ConnectorError(Exception):
    pass

@retry(3)
def connection(logger, globopts, scheme, host, url, msgprefix):
    try:
        if scheme.startswith('https'):
            if eval(globopts['AuthenticationVerifyServerCert'.lower()]):
                verify_cert(host, int(globopts['ConnectionTimeout'.lower()]),
                                            globopts['AuthenticationCAPath'.lower()], globopts['AuthenticationCAFile'.lower()])
            conn = httplib.HTTPSConnection(host, 443,
                                            globopts['AuthenticationHostKey'.lower()],
                                            globopts['AuthenticationHostCert'.lower()],
                                            timeout=int(globopts['ConnectionTimeout'.lower()]))
        else:
            conn = httplib.HTTPConnection(host, 80, timeout=int(globopts['ConnectionTimeout'.lower()]))

        conn.request('GET', url)
        resp = conn.getresponse()

        if resp.status != 200:
            raise httplib.HTTPException('Response: %s %s' % (resp.status, resp.reason))

        return resp

    except(SSLError, socket.error, socket.timeout) as e:
        logger.warn('%sConnection error %s - %s' % (msgprefix + ' ' if msgprefix else '',
                                                     scheme + '://' + host,
                                                     error_message(e)))
        raise e

    except httplib.HTTPException as e:
        raise e



def parse_xml(logger, response, method, objname):
    try:
        doc = xml.dom.minidom.parseString(response.read())

    except ExpatError as e:
        logger.error(objname + ': Error parsing XML feed %s - %s' % (method, error_message(e)))
        raise ConnectorError()

    except Exception as e:
        logger.error(objname + ': Error %s - %s' % (method, error_message(e)))
        raise e

    else:
        return doc

def parse_json(logger, response, method, objname):
    try:
        doc = json.loads(response.read())

    except ValueError as e:
        logger.error(objname + ': Error parsing JSON feed %s - %s' % (method, error_message(e)))
        raise ConnectorError()

    except Exception as e:
        logger.error(objname + ': Error %s - %s' % (method, error_message(e)))
        raise e

    else:
        return doc

def verify_cert(host, timeout, capath, cafile):
    def verify_cert(host, ca, timeout):
        server_ctx = Context(TLSv1_METHOD)
        server_cert_chain = []

        if os.path.isdir(ca):
            server_ctx.load_verify_locations(None, ca)
        else:
            server_ctx.load_verify_locations(ca, None)

        def verify_cb(conn, cert, errnum, depth, ok):
            server_cert_chain.append(cert)
            return ok
        server_ctx.set_verify(VERIFY_PEER, verify_cb)

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setblocking(1)
        sock.settimeout(timeout)
        sock.connect((host, 443))

        server_conn = Connection(server_ctx, sock)
        server_conn.set_connect_state()

        def iosock_try():
            ok = True
            try:
                server_conn.do_handshake()
                sleep(0.5)
            except SSLWantReadError as e:
                ok = False
                pass
            except Exception as e:
                raise e
            return ok

        try:
            while True:
                if iosock_try():
                    break

            server_subject = server_cert_chain[-1].get_subject()
            if host != server_subject.CN:
                raise SSLError('Server certificate CN does not match %s' % host)

        except SSLError as e:
            raise e
        finally:
            server_conn.shutdown()
            server_conn.close()

        return True

    try:
        verify_cert(host, capath, timeout)
    except SSLError:
        verify_cert(host, cafile, timeout)


    return True
