import base64
import json
import requests
import socket
import xml.dom.minidom

from argo_egi_connectors.helpers import retry

from xml.parsers.expat import ExpatError
from urllib.parse import urlparse


class ConnectorError(Exception):
    pass


@retry
def connection(logger, msgprefix, globopts, scheme, host, url, custauth=None):
    try:
        buf = None

        headers = {}
        if custauth and msgprefix == 'WebAPI':
            headers = {'x-api-key': custauth['WebApiToken'.lower()],
                       'Accept': 'application/json'}
        elif msgprefix != 'PoemReader' and custauth and eval(custauth['AuthenticationUsePlainHttpAuth'.lower()]):
            userpass = base64.b64encode(custauth['AuthenticationHttpUser'.lower()] + ':'
                                        + custauth['AuthenticationHttpPass'.lower()])
            headers = {'Authorization': 'Basic ' + userpass}

        if scheme.startswith('https'):
            response = requests.get('https://' + host + url, headers=headers,
                                    cert=(globopts['AuthenticationHostCert'.lower()],
                                          globopts['AuthenticationHostKey'.lower()]),
                                    verify=eval(globopts['AuthenticationVerifyServerCert'.lower()]),
                                    timeout=int(globopts['ConnectionTimeout'.lower()]))
            response.raise_for_status()
        else:
            response = requests.get('http://' + host + url, headers=headers,
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

    except requests.exceptions.SSLError as e:
        if (getattr(e, 'args', False) and type(e.args) == tuple and
            type(e.args[0]) == str and 'timed out' in e.args[0]):
            raise e
        else:
            if getattr(logger, 'job', False):
                msg = '{}Customer:{} Job:{} SSL Error {} - {}'.format(msgprefix + ' ' if msgprefix else '',
                                                                      logger.customer, logger.job,
                                                                      scheme + '://' + host + url,
                                                                      repr(e))
            else:
                msg = '{}Customer:{} SSL Error {} - {}'.format(msgprefix + ' ' if msgprefix else '',
                                                               logger.customer,
                                                               scheme + '://' + host + url,
                                                               repr(e))

            logger.critical(msg)
        return False

    except(socket.error, socket.timeout) as e:
        if getattr(logger, 'job', False):
            msg = '{}Customer:{} Job:{} Connection error {} - {}'.format(msgprefix + ' ' if msgprefix else '',
                                                                         logger.customer, logger.job,
                                                                         scheme + '://' + host + url,
                                                                         repr(e))
        else:
            msg = '{}Customer:{} Connection error {} - {}'.format(msgprefix + ' ' if msgprefix else '',
                                                                  logger.customer,
                                                                  scheme + '://' + host + url,
                                                                  repr(e))
        logger.warn(msg)
        raise e

    except requests.exceptions.RequestException as e:
        if getattr(logger, 'job', False):
            msg = '{}Customer:{} Job:{} HTTP error {} - {}'.format(msgprefix + ' ' if msgprefix else '',
                                                                   logger.customer, logger.job,
                                                                   scheme + '://' + host + url,
                                                                   repr(e))
        else:
            msg = '{}Customer:{} HTTP error {} - {}'.format(msgprefix + ' ' if msgprefix else '',
                                                            logger.customer,
                                                            scheme + '://' + host + url,
                                                            repr(e))
        logger.warn(msg)
        raise e

    except Exception as e:
        if getattr(logger, 'job', False):
            msg = '{}Customer:{} Job:{} Error {} - {}'.format(msgprefix + ' ' if msgprefix else '',
                                                              logger.customer, logger.job,
                                                              scheme + '://' + host + url,
                                                              repr(e))
        else:
            msg = '{}Customer:{} Error {} - {}'.format(msgprefix + ' ' if msgprefix else '',
                                                       logger.customer,
                                                       scheme + '://' + host + url,
                                                       repr(e))
        logger.warn(msg)
        return False


def parse_xml(logger, objname, globopts, buf, method):
    try:
        doc = xml.dom.minidom.parseString(buf)

    except ExpatError as e:
        if getattr(logger, 'job', False):
            msg = '{} Customer:{} Job:{} : Error parsing XML feed {} - {}'.format(objname, logger.customer, logger.job, method, repr(e))
        else:
            msg = '{} Customer:{} : Error parsing XML feed {} - {}'.format(objname, logger.customer, method, repr(e))
        logger.error(msg)
        raise ConnectorError()

    except Exception as e:
        if getattr(logger, 'job', False):
            msg = '{} Customer:{} Job:{} : Error {} - {}'.format(objname, logger.customer, logger.job, method, repr(e))
        else:
            msg = '{} Customer:{} : Error {} - {}'.format(objname, logger.customer, method, repr(e))
        logger.error(msg)
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
