import base64
import httplib
import json
import os
import requests
import socket
import xml.dom.minidom

from argo_egi_connectors.helpers import retry

from OpenSSL.SSL import TLSv1_METHOD, Context, Connection
from OpenSSL.SSL import VERIFY_PEER
from OpenSSL.SSL import WantReadError as SSLWantReadError
from ssl import SSLError
from time import sleep
from xml.parsers.expat import ExpatError
from urlparse import urlparse


class ConnectorError(Exception):
    pass


@retry
def connection(logger, msgprefix, globopts, scheme, host, url, custauth=None):
    try:
        buf = None

        headers = {}
        if custauth and eval(custauth['AuthenticationUsePlainHttpAuth'.lower()]):
            userpass = base64.b64encode(custauth['AuthenticationHttpUser'.lower()] + ':' \
                                        + custauth['AuthenticationHttpPass'.lower()])
            headers={'Authorization': 'Basic ' + userpass}

        if scheme.startswith('https'):
            if eval(globopts['AuthenticationVerifyServerCert'.lower()]):
                verify_cert(host, int(globopts['ConnectionTimeout'.lower()]),
                            globopts['AuthenticationCAPath'.lower()],
                            globopts['AuthenticationCAFile'.lower()])
            response = requests.get('https://'+ host + url, headers=headers,
                                    cert=(globopts['AuthenticationHostCert'.lower()],
                                          globopts['AuthenticationHostKey'.lower()]),
                                    verify=False,
                                    timeout=int(globopts['ConnectionTimeout'.lower()]))
            response.raise_for_status()
        else:
            response = requests.get('http://'+ host + url, headers=headers,
                                    timeout=int(globopts['ConnectionTimeout'.lower()]))


        if response.status_code >= 300 and response.status_code < 400:
            headers = response.headers
            location = filter(lambda h: 'location' in h[0], headers)
            if location:
                redir = urlparse(location[0][1])
            else:
                raise requests.exceptions.RequestException('No Location header set for redirect')

            return connection(logger, msgprefix, globopts, scheme, redir.netloc, redir.path + '?' + redir.query, custauth=custauth)

        elif response.status_code == 200:
            buf = response.content
            if not buf:
                raise requests.exceptions.RequestException('Empty response')

        else:
            raise requests.exceptions.RequestException('response: %s %s' % (response.status_code, response.reason))

        return buf

    except SSLError as e:
        if (getattr(e, 'args', False) and type(e.args) == tuple
            and type(e.args[0]) == str
            and 'timed out' in e.args[0]):
            raise e
        else:
            logger.critical('%sCustomer:%s Job:%s SSL Error %s - %s' % (msgprefix + ' ' if msgprefix else '',
                                                                        logger.customer, logger.job,
                                                                        scheme + '://' + host + url,
                                                                        repr(e)))
        return False

    except(socket.error, socket.timeout) as e:
        logger.warn('%sCustomer:%s Job:%s Connection error %s - %s' % (msgprefix + ' ' if msgprefix else '',
                                                                       logger.customer, logger.job,
                                                                       scheme + '://' + host + url,
                                                                       repr(e)))
        raise e

    except requests.exceptions.RequestException as e:
        logger.warn('%sCustomer:%s Job:%s HTTP error %s - %s' % (msgprefix + ' ' if msgprefix else '',
                                                                 logger.customer, logger.job,
                                                                 scheme + '://' + host + url,
                                                                 repr(e)))
        raise e

    except Exception as e:
        logger.critical('%sCustomer:%s Job:%s Error %s - %s' % (msgprefix + ' ' if msgprefix else '',
                                                                logger.customer, logger.job,
                                                                scheme + '://' + host + url,
                                                                repr(e)))
        return False


def parse_xml(logger, objname, globopts, buf, method):
    try:
        doc = xml.dom.minidom.parseString(buf)

    except ExpatError as e:
        logger.error(objname + ' Customer:%s Job:%s : Error parsing XML feed %s - %s' % (logger.customer, logger.job, method, repr(e)))
        raise ConnectorError()

    except Exception as e:
        logger.error(objname + ' Customer:%s Job:%s : Error %s - %s' % (logger.customer, logger.job, method, repr(e)))
        raise e

    else:
        return doc


def parse_json(logger, objname, globopts, buf, method):
    try:
        doc = json.loads(buf)

    except ValueError as e:
        logger.error(objname + ' Customer:%s Job:%s : Error parsing JSON feed %s - %s' % (logger.customer, logger.job, method, repr(e)))
        raise ConnectorError()

    except Exception as e:
        logger.error(objname + ' Customer:%s Job:%s : Error %s - %s' % (logger.customer, logger.job, method, repr(e)))
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
